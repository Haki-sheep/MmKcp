using System;
using System.Collections.Generic;

namespace MmKcp;

/// <remarks>
/// 估算动态超时 累计快速重传证据并处理到期重发
/// </remarks>
public sealed partial class Kcp
{
    /// <summary> 初始重传等待时间 单位为毫秒 </summary>
    private const uint ResendDelay = 200;
    /// <summary> 触发快速重传需要的后续段确认次数 零表示关闭 </summary>
    private uint fastResendThreshold = 2;
    /// <summary> 快速重传的累计发送次数上限 </summary>
    private const uint FastRetransmitLimit = 5;
    /// <summary> 达到此发送次数后标记死链 </summary>
    private const uint DeadLinkLimit = 20;
    /// <summary> 已占用发送窗口的段 包含待首次发送和等待确认的记录 </summary>
    private readonly Dictionary<uint, UnacknowledgedSegment> sendBufferDict = new();
    /// <summary> 平滑后的往返耗时 单位为毫秒 </summary>
    private long smoothedRtt;
    /// <summary> 往返耗时的波动量 单位为毫秒 </summary>
    private long rttVariation;
    /// <summary> 是否已收到首个有效耗时样本 </summary>
    private bool hasRttSample;
    /// <summary> 当前估算的重传等待时间 单位为毫秒 </summary>
    private uint retransmissionTimeout = ResendDelay;
    /// <summary> 重传等待时间的下限 单位为毫秒 </summary>
    private uint minimumRetransmissionTimeout = 100;
    /// <summary> 控制超时下限和退避方式 </summary>
    private int noDelayMode;

    /// <summary>
    /// 统一处理窗口内段的首次发送 超时重传和快速重传
    /// </summary>
    private void FlushResends(uint now, ref int bufferedLength, ref int pacingBudget,
        uint inFlight, out uint fastRetransmits, out bool hadTimeout)
    {
        fastRetransmits = 0;
        hadTimeout = false;
        // 遍历已进入发送窗口的段
        foreach (var entry in sendBufferDict)
        {
            var unacknowledged = entry.Value;
            bool first = unacknowledged.Transmissions == 0;
            bool timedOut = !first && Difference(now, unacknowledged.ResendAt) >= 0;
            // 例如 0 未确认但 1 和 2 已确认 FastAck 达到 2 后无需等到 ResendAt
            bool fast = !first && fastResendThreshold > 0 &&
                unacknowledged.FastAck >= fastResendThreshold &&
                unacknowledged.Transmissions <= FastRetransmitLimit;
            if (!first && !timedOut && !fast)
                continue;
            int length = unacknowledged.Segment.DataList.Length;
            // 预算不足时保留原计时和发送次数 尚未输出的数据不能算作已经发出
            if (pacingBudget >= 0 && pacingBudget < length)
                break;

            if (first)
                unacknowledged.Timeout = retransmissionTimeout;
            else if (timedOut)
            {
                // 超时后延长等待时间 快速重传沿用本段等待时间
                // 例如当前估算和本段等待均为 200 普通模式下次等 400 模式 1 下次等 300 毫秒
                uint growth = noDelayMode == 0
                    ? Math.Max(unacknowledged.Timeout, retransmissionTimeout)
                    : noDelayMode == 1 ? unacknowledged.Timeout / 2 : retransmissionTimeout / 2;
                unacknowledged.Timeout = Math.Min(unacknowledged.Timeout + growth, 60000u);
                hadTimeout = true;
            }
            else
                fastRetransmits++;

            // 普通模式首次发送额外等待八分之一 RTO 减少边界时刻过早重传
            uint extraDelay = first && noDelayMode == 0 ? retransmissionTimeout >> 3 : 0;
            unacknowledged.FastAck = 0;
            var Segment = unacknowledged.Segment;
            unacknowledged.Segment = CreateSegment(EKcpCommand.Push, Segment.Header.RemainingFragmentCount,
                now, Segment.Header.SendSequence, Segment.DataList);
            // 重传时间等于当前时间加本段等待时间 本次重发后重新等待 ACK
            unacknowledged.ResendAt = unchecked(now + unacknowledged.Timeout + extraDelay);
            unacknowledged.Transmissions++;
            congestionControl?.PacketSent?.Invoke(this, entry.Key, now, length,
                inFlight, unacknowledged.Transmissions);
            // 重发与首次发送共用组包路径
            OutputPush(unacknowledged, now, ref bufferedLength);
            if (pacingBudget >= 0)
                pacingBudget -= length;
            // 此处只标记死链 是否断开连接由持有 Kcp 的外层决定
            if (unacknowledged.Transmissions >= DeadLinkLimit)
                IsDeadLink = true;
        }
    }

    /// <summary>
    /// 后续段被确认时 为仍在等待的前序段累计确认次数
    /// </summary>
    private void CountFastAcks(uint sequence, uint timestamp)
    {
        if (Difference(sequence, sendUnacknowledged) < 0 || Difference(sequence, SendNext) >= 0)
            return;
        foreach (var entry in sendBufferDict)
        {
            var unacknowledged = entry.Value;
            // 时间戳早于本段最近发送时刻的旧 ACK 不能作为本次发送丢失的证据
            if (unacknowledged.Transmissions > 0 && Difference(sequence, entry.Key) > 0 &&
                Difference(timestamp, unacknowledged.Segment.Header.Timestamp) >= 0)
                unacknowledged.FastAck++;
        }
    }

    /// <summary>
    /// 根据往返耗时的平均值和波动量计算重传等待时间
    /// </summary>
    private void UpdateRtt(int rtt)
    {
        if (!hasRttSample)
        {
            // 首个样本没有历史波动可比较 先取耗时的一半作为初始波动量
            hasRttSample = true;
            smoothedRtt = rtt;
            rttVariation = rtt / 2;
        }
        else
        {
            // 先与旧平均值比较 新样本占平均值的八分之一 占波动量的四分之一
            long delta = Math.Abs(rtt - smoothedRtt);
            rttVariation = (3 * rttVariation + delta) / 4;
            smoothedRtt = Math.Max(1, (7 * smoothedRtt + rtt) / 8);
        }
        // 平均耗时之外预留波动余量 且至少覆盖一轮刷新间隔
        long timeout = smoothedRtt + Math.Max(flushInterval, 4 * rttVariation);
        retransmissionTimeout = (uint)Math.Clamp(timeout, minimumRetransmissionTimeout, 60000);
        congestionControl?.RttMeasured?.Invoke(this, rtt);
    }
}
