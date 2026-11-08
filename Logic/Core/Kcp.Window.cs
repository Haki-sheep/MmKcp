using System;

namespace MmKcp;

/// <remarks>
/// 管理发送和接收窗口 零窗口探测以及内置拥塞控制
/// </remarks>
public sealed partial class Kcp
{
    /// <summary> 首次零窗口探测等待时间 单位为毫秒 </summary>
    private const uint InitialProbeWait = 5000;
    /// <summary> 零窗口探测最大等待时间 单位为毫秒 </summary>
    private const uint MaximumProbeWait = 120000;
    /// <summary> 拥塞避免切换阈值的最小段数 </summary>
    private const uint MinimumSlowStartThreshold = 2;
    /// <summary> 允许在途的发送序号范围 </summary>
    private uint sendWindow = 32;
    /// <summary> 本端接收队列容量 单位为段 </summary>
    private uint receiveWindow = DefaultReceiveWindow;
    /// <summary> 对方最近通告的剩余接收窗口 </summary>
    private uint remoteWindow = DefaultReceiveWindow;
    /// <summary> 按字节累计的拥塞窗口增长量 </summary>
    private ulong congestionBytes = 1376;
    /// <summary> 慢启动转为拥塞避免的段数阈值 </summary>
    private uint slowStartThreshold = MinimumSlowStartThreshold;
    /// <summary> 配置后发送不受内置拥塞窗口限制 </summary>
    private bool disableCongestionWindow;
    /// <summary> 下次零窗口探测时刻 </summary>
    private uint probeTime;
    /// <summary> 零窗口探测退避间隔 单位为毫秒 </summary>
    private uint probeWait;
    /// <summary> 需要在下次刷新通告当前接收窗口 </summary>
    private bool tellWindow;

    public uint SendWindow => sendWindow;
    public uint ReceiveWindow => receiveWindow;
    public uint RemoteWindow => remoteWindow;
    public uint CongestionWindow { get; set; } = 1;
    private ushort AvailableReceiveWindow =>
        (ushort)Math.Max(0L, (long)receiveWindow - receiveQueueList.Count);

    #region 窗口配置与探测

    /// <summary>
    /// 配置窗口段数 接收窗口至少能容纳原版支持的最大消息
    /// </summary>
    public void SetWindowSize(int sendWindow, int receiveWindow)
    {
        EnsureActive();
        if (receiveWindow > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(receiveWindow));
        if (sendWindow > 0)
            this.sendWindow = (uint)sendWindow;
        if (receiveWindow > 0)
        {
            this.receiveWindow = (uint)Math.Max(DefaultReceiveWindow, receiveWindow);
            MoveContiguousSegments();
            tellWindow = true;
        }
    }

    private uint GetSendWindow()
    {
        // 例如本端允许 32 段 对方能收 128 段 拥塞窗口为 4 最终只准入 4 段
        uint window = Math.Min(sendWindow, remoteWindow);
        // 安装自定义策略后仍使用它设置的拥塞窗口 关闭内置限制的选项不绕过自定义策略
        return congestionControl != null || !disableCongestionWindow
            ? Math.Min(window, CongestionWindow) : window;
    }

    private void FlushWindowProbes(uint now, ref int bufferedLength)
    {
        if (remoteWindow != 0)
        {
            probeWait = 0;
            probeTime = 0;
        }
        else if (probeWait == 0)
        {
            // 对方窗口为零后先等 5 秒 再询问它现在是否有接收空间
            probeWait = InitialProbeWait;
            probeTime = unchecked(now + probeWait);
        }
        else if (Difference(now, probeTime) >= 0)
        {
            // 继续未恢复时探测间隔每次增加一半 即使恢复通告丢失也会再次询问
            probeWait = Math.Min(probeWait + probeWait / 2, MaximumProbeWait);
            probeTime = unchecked(now + probeWait);
            AppendSegment(ref bufferedLength, EKcpCommand.WindowAsk, 0, 0, 0,
                Array.Empty<byte>());
        }

        // 业务读取腾出接收空间或收到 WindowAsk 后 主动通告当前窗口
        if (tellWindow)
        {
            AppendSegment(ref bufferedLength, EKcpCommand.WindowTell, 0, 0, 0,
                Array.Empty<byte>());
            tellWindow = false;
        }
    }

    #endregion

    #region 拥塞窗口调整

    private void OnAcknowledged(uint previousUna, uint acknowledgedBytes, uint previousInFlight)
    {
        // 只有最早未确认序号前移才增长窗口 重复 ACK 或仅确认后续段不会在此增长
        if (Difference(sendUnacknowledged, previousUna) <= 0)
            return;
        if (congestionControl?.Acknowledged is { } acknowledged)
        {
            acknowledged(this, unchecked(sendUnacknowledged - previousUna),
                acknowledgedBytes, previousInFlight);
            return;
        }
        GrowCongestionWindow();
    }

    private void GrowCongestionWindow()
    {
        if (CongestionWindow >= remoteWindow)
            return;
        uint segmentSize = (uint)MaxSegmentSize;
        if (CongestionWindow < slowStartThreshold)
        {
            // 慢启动阶段每次有效推进增加一段 不必积累多次确认
            CongestionWindow++;
            congestionBytes += segmentSize;
        }
        else
        {
            // 拥塞避免先积累字节额度 凑够下一整段再扩窗 当前窗口越大单次增长越少
            congestionBytes = Math.Max(congestionBytes, segmentSize);
            congestionBytes += (ulong)segmentSize * segmentSize / congestionBytes + segmentSize / 16;
            if ((ulong)(CongestionWindow + 1) * segmentSize <= congestionBytes)
                CongestionWindow = (uint)((congestionBytes + segmentSize - 1) / segmentSize);
        }
        if (CongestionWindow > remoteWindow)
        {
            CongestionWindow = remoteWindow;
            congestionBytes = (ulong)remoteWindow * segmentSize;
        }
    }

    private void UpdateCongestionAfterFlush(uint fastRetransmits, bool hadTimeout,
        uint previousWindow, uint inFlight)
    {
        if (fastRetransmits > 0)
        {
            if (congestionControl?.FastRetransmit is { } fastRetransmit)
                fastRetransmit(this, fastRetransmits, inFlight, previousWindow);
            else
            {
                // 快速重传说明后续段仍能送达 将序号跨度减半并保留快速恢复余量
                slowStartThreshold = Math.Max(unchecked(SendNext - sendUnacknowledged) / 2,
                    MinimumSlowStartThreshold);
                CongestionWindow = slowStartThreshold + fastResendThreshold;
                congestionBytes = (ulong)CongestionWindow * (uint)MaxSegmentSize;
            }
        }
        // 同轮还有超时时在快速恢复之后处理 让更保守的超时结果决定后续窗口
        if (hadTimeout)
        {
            if (congestionControl?.Timeout is { } timeout)
                timeout(this, previousWindow);
            else
            {
                // 超时未等到足够反馈 退回一段重新慢启动 阈值按本轮原拥塞窗口减半
                slowStartThreshold = Math.Max(previousWindow / 2, MinimumSlowStartThreshold);
                CongestionWindow = 1;
                congestionBytes = (uint)MaxSegmentSize;
            }
        }
        if (CongestionWindow < 1)
        {
            CongestionWindow = 1;
            congestionBytes = (uint)MaxSegmentSize;
        }
    }

    #endregion
}
