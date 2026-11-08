using System;

namespace MmKcp;

/// <summary>
/// 会话构造和协议时钟驱动
/// 发送时，业务把原始字节交给 Send 并拆成段入队 Update 到点后调用 Flush 刷新段头
/// 通过 Output 把报文交给外层，再由外层用 UDP 发出去。
/// 接收时，外层把 UDP 收到的报文交给 Input。KCP 解析、去重、按序排好。
/// 业务再调用 Recv()，等一条消息的分片收齐了，才拿到完整的原始字节。
/// 段:加了kcp头,Kcp传递段
/// 片:段携带的业务负载数据
/// </summary>
public sealed partial class Kcp : IDisposable
{
    /// <summary> 原版消息模式允许的默认接收窗口段数 </summary>
    private const int DefaultReceiveWindow = 128;
    /// <summary> 刷新间隔(ms) </summary>
    private uint flushInterval = 100;
    /// <summary> 拼好后的回调 输出内存可由调用方保留 </summary>
    private readonly Action<ReadOnlyMemory<byte>> Output;
    /// <summary> 下一次刷新时刻 单位为毫秒 </summary>
    private uint NextFlushTime;
    /// <summary> 首次刷新时刻是否已设置 </summary>
    private bool HasUpdated;
    /// <summary> 当前协议时间 单位为单调毫秒计数 </summary>
    private uint currentTime;
    /// <summary> 释放后禁止继续驱动会话 </summary>
    private bool disposed;
    /// <summary> 流模式只保证字节顺序 不保留 Send 的消息边界 </summary>
    private bool streamMode;
    /// <summary> 自定义拥塞策略的生命周期由会话负责 </summary>
    private KcpCongestionControl? congestionControl;

    // 会话ID
    public uint ConversationId { get; }
    public int PendingSendCount => SendQueueList.Count + sendBufferDict.Count;
    public bool IsDeadLink { get; private set; }
    public uint SmoothedRttMilliseconds => (uint)smoothedRtt;
    /// <summary> 启用后按字节顺序发送 切换前须清空待发送和待确认的段 </summary>
    public bool StreamMode
    {
        get => streamMode;
        set
        {
            EnsureActive();
            if (streamMode != value && PendingSendCount > 0)
                throw new InvalidOperationException("切换流模式前必须清空待发送和待确认的段");
            streamMode = value;
        }
    }

    #region 生命周期与配置

    /// <summary>
    /// 输出回调同步接收完整数据报 输出内存可保留 会话由调用方串行驱动 回调不得重入本会话
    /// 默认启用快速模式 每 10 毫秒刷新 快速重传阈值为 2 关闭内置拥塞限制
    /// </summary>
    public Kcp(uint conversationId, Action<ReadOnlyMemory<byte>> output)
    {
        ConversationId = conversationId;
        Output = output ?? throw new ArgumentNullException(nameof(output));
        // 帧同步小消息优先降低排队延迟 收发窗口仍生效 业务发送量由外层控制
        SetNoDelay(1, 10, 2, 1);
    }

    /// <summary>
    /// 配置刷新间隔和重传策略 参数为负数时保留当前配置
    /// 最后一项为零时恢复内置拥塞限制 为一时关闭该限制
    /// </summary>
    public void SetNoDelay(int mode, int intervalMilliseconds, int fastResend, int disableCongestionWindow)
    {
        EnsureActive();
        if (mode >= 0)
        {
            noDelayMode = mode;
            minimumRetransmissionTimeout = mode == 0 ? 100u : 30u;
        }
        if (intervalMilliseconds >= 0)
            flushInterval = (uint)Math.Clamp(intervalMilliseconds, 10, 5000);
        if (fastResend >= 0)
            fastResendThreshold = (uint)fastResend;
        if (disableCongestionWindow >= 0)
            this.disableCongestionWindow = disableCongestionWindow != 0;
    }

    /// <summary>
    /// 替换拥塞策略 传入 null 移除自定义策略 每个策略实例归当前会话独占
    /// </summary>
    public void SetCongestionControl(KcpCongestionControl? strategy)
    {
        EnsureActive();
        if (ReferenceEquals(congestionControl, strategy))
            return;
        var previous = congestionControl;
        // 先解除旧策略的持有关系 避免释放回调抛错后下次再次释放同一策略
        congestionControl = null;
        previous?.Release?.Invoke(this);
        strategy?.Initialize?.Invoke(this);
        congestionControl = strategy;
        if (strategy == null)
        {
            CongestionWindow = Math.Max(1u, CongestionWindow);
            congestionBytes = (ulong)CongestionWindow * (uint)MaxSegmentSize;
        }
    }

    public void Dispose()
    {
        if (disposed)
            return;
        // 先标记并清空会话 即使外部 Release 抛错也不能继续使用或重复释放
        disposed = true;
        var strategy = congestionControl;
        congestionControl = null;
        SendQueueList.Clear();
        sendBufferDict.Clear();
        receiveQueueList.Clear();
        receiveBufferDict.Clear();
        pendingAckList.Clear();
        outputBufferList = Array.Empty<byte>();
        strategy?.Release?.Invoke(this);
    }

    private void EnsureActive()
    {
        if (disposed)
            throw new ObjectDisposedException(nameof(Kcp));
    }

    #endregion

    #region 协议时钟与刷新

    /// <summary>
    /// 刷新队列中的分片
    /// </summary>
    /// <param name="now">与 Input 同源的单调毫秒计数低三十二位</param>
    public void Update(uint now)
    {
        EnsureActive();
        currentTime = now;
        if (!HasUpdated)
        {
            HasUpdated = true;
            NextFlushTime = now;
        }

        // unchecked表示32位溢出回绕
        // 比如 now = 1099，NextFlushTime = 1100 ,uint无法表示1099-1100的差值,所以会溢出
        // 所以-1 先回绕成 0xFFFFFFFF，再转成 int 得到 -1，然后小于0，所以不刷新
        int elapsed = Difference(now, NextFlushTime);
        // 长时间暂停后重新对齐刷新时刻 避免沿用暂停前的时间表
        if (elapsed >= 10000 || elapsed < -10000)
        {
            NextFlushTime = now;
            elapsed = 0;
        }
        if (elapsed < 0)
            return;

        NextFlushTime = unchecked(NextFlushTime + flushInterval);
        // 迟到不足一轮时保留原节奏 跨过多轮时只刷新一次并从当前时刻重新计时
        if (Difference(now, NextFlushTime) >= 0)
            NextFlushTime = unchecked(now + flushInterval);
        Flush(now);
    }

    /// <summary>
    /// 返回没有新增发送和输入时下一次有效刷新的时间
    /// </summary>
    public uint Check(uint now)
    {
        EnsureActive();
        int delay = Difference(NextFlushTime, now);
        return !HasUpdated || delay <= 0 || delay > 10000 ? now : NextFlushTime;
    }

    /// <summary>
    /// 使用当前协议时间立即刷新 首次 Update 前调用无效
    /// </summary>
    public void Flush()
    {
        EnsureActive();
        if (HasUpdated)
            Flush(currentTime);
    }

    /// <summary>
    /// Kcp将消息加头后通过 Output交给外部
    /// </summary>
    /// <param name="now">当前时间 单位为毫秒</param>
    private void Flush(uint now)
    {
        congestionControl?.Tick?.Invoke(this);
        int pacingBudget = congestionControl?.GetPacingBudget?.Invoke(this) ?? -1;
        uint previousWindow = CongestionWindow;
        int bufferedLength = 0;
        // 即使对方窗口为零或数据预算不足 ACK 和窗口通告仍需发出
        FlushAcks(ref bufferedLength);
        FlushWindowProbes(now, ref bufferedLength);
        uint window = GetSendWindow();
        // 先为窗口内的新分片分配序号 首次发送和重传再统一检查发送预算
        FlushNewSegments(now, window);
        uint inFlight = (uint)sendBufferDict.Count;
        if (SendQueueList.Count == 0 && inFlight < window)
            congestionControl?.AppLimited?.Invoke(this, inFlight);
        FlushResends(now, ref bufferedLength, ref pacingBudget, inFlight,
            out uint fastRetransmits, out bool hadTimeout);
        Emit(bufferedLength);
        // 根据本轮实际发生的重传调整窗口 调整结果作用于后续发送
        UpdateCongestionAfterFlush(fastRetransmits, hadTimeout, previousWindow, inFlight);
    }

    private static int Difference(uint left, uint right) => unchecked((int)(left - right));

    #endregion
}
