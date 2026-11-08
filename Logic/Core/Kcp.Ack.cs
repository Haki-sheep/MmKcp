using System;
using System.Collections.Generic;

namespace MmKcp;

/// <remarks>
/// 记录和发送 ACK 处理单段确认与累计确认
/// </remarks>
public sealed partial class Kcp
{
    /// <summary> 等待发回对方的 ACK 保存对方段序号与原发送时间戳 </summary>
    private readonly List<(uint Sequence, uint Timestamp)> pendingAckList = new();
    /// <summary> 最早尚未确认的发送序号 无在途段时等于 SendNext </summary>
    private uint sendUnacknowledged;

    /// <summary>
    /// 发送待发 ACK 带回对方的段序号和发送时间
    /// </summary>
    private void FlushAcks(ref int bufferedLength)
    {
        foreach (var ack in pendingAckList)
            AppendSegment(ref bufferedLength, EKcpCommand.Ack, 0, ack.Timestamp,
                ack.Sequence, Array.Empty<byte>());
        pendingAckList.Clear();
    }

    /// <summary>
    /// 清理已确认的段 更新往返耗时和后续段的确认次数
    /// </summary>
    private uint InputAck(KcpHeader header, uint now, UnacknowledgedSegment? confirmed)
    {
        // 同一段只提供一次快速重传证据
        if (confirmed == null)
            return 0;
        int rtt = Difference(now, header.Timestamp);
        // ACK 回显原发送时刻 允许测量被确认的旧发送并适应慢链路
        if (rtt >= 0)
            UpdateRtt(rtt);
        uint length = 0;
        bool removed = sendBufferDict.Remove(header.SendSequence);
        // 先维护最早未确认序号 再通知外部策略 避免回调抛错后序号仍指向已删除记录
        RefreshSendUnacknowledged();
        if (removed)
            length = NotifyAcknowledged(header.SendSequence, confirmed,
                Math.Max(-1, Difference(now, confirmed.Segment.Header.Timestamp)));
        if (rtt >= 0)
            CountFastAcks(header.SendSequence, header.Timestamp);
        return length;
    }

    /// <summary>
    /// 发送端根据接收端的反馈，核对哪些消息段已经送达,并从发送缓冲区中清理掉
    /// </summary>
    /// <param name="peerReceiveNext">对方下一个期待的段序号</param>
    private uint ApplyCumulativeAck(uint peerReceiveNext)
    {
        // 算出这次累计确认要往前走几格  = 对方下一个期待的段序号 - 自己最早尚未确认的发送序号
        int confirmedCount = Difference(peerReceiveNext, sendUnacknowledged);
        // 算出从当前清理位置到已发送范围末尾，一共有几格
        int sentCount = Difference(SendNext, sendUnacknowledged);
        // 如果确认的段数小于0或者大于发送的段数,则返回
        if (confirmedCount <= 0 || confirmedCount > sentCount)
            return 0;
        uint acknowledgedBytes = 0;
        // 对方期待 3 表示 0 1 2 已按序收到 这里只清理 3 之前的段
        while (Difference(peerReceiveNext, sendUnacknowledged) > 0)
        {
            uint sequence = sendUnacknowledged;
            sendBufferDict.TryGetValue(sequence, out var unacknowledged);
            sendBufferDict.Remove(sequence);
            sendUnacknowledged = unchecked(sendUnacknowledged + 1);
            // 已被单独 ACK 移除的序号也要跳过 因此游标可能越过本次累计确认边界
            RefreshSendUnacknowledged();
            if (unacknowledged != null)
                acknowledgedBytes += NotifyAcknowledged(sequence, unacknowledged, -1);
        }
        return acknowledgedBytes;
    }

    private void RefreshSendUnacknowledged()
    {
        while (sendUnacknowledged != SendNext && !sendBufferDict.ContainsKey(sendUnacknowledged))
            sendUnacknowledged = unchecked(sendUnacknowledged + 1);
    }

    private uint NotifyAcknowledged(uint sequence, UnacknowledgedSegment segment, int rtt)
    {
        congestionControl?.PacketAcknowledged?.Invoke(this, sequence, segment.Segment.Header.Timestamp,
            segment.Segment.DataList.Length, rtt, segment.Transmissions);
        return (uint)segment.Segment.DataList.Length;
    }
}
