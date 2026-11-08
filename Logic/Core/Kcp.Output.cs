using System;

namespace MmKcp;

/// <remarks>
/// 业务消息分片入队 将数据段编码后交给外层
/// </remarks>
public sealed partial class Kcp
{
    /// <summary> 数据报单次传输上限 单位为字节 </summary>
    private byte[] outputBufferList = new byte[1400];
    /// <summary> 尚未分配发送序号的段 </summary>
    private readonly KcpSegmentQueue SendQueueList = new();
    /// <summary> 下一个待分配的发送序号 </summary>
    private uint SendNext;

    public int Mtu => outputBufferList.Length;
    // 每一个分片可用最大业务负载字节数
    private int MaxSegmentSize => Mtu - KcpHeader.HeadSize;

    #region 发送与分片配置

    /// <summary>
    /// 调整报文上限 有待发和待确认段时禁止降低 MTU
    /// </summary>
    public void SetMtu(int mtu)
    {
        EnsureActive();
        if (mtu < 50 || mtu > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(mtu));
        if (mtu < Mtu && PendingSendCount > 0)
            throw new InvalidOperationException("降低 MTU 前必须清空待发送和待确认的段");
        if (mtu != Mtu)
        {
            outputBufferList = new byte[mtu];
            congestionBytes = (ulong)CongestionWindow * (uint)MaxSegmentSize;
        }
    }

    /// <summary>
    /// 复制消息并分片入队 返回排队字节数 分片过多时返回负二
    /// 流模式可能仅接受部分数据 调用方按返回长度继续发送
    /// </summary>
    public int Send(ReadOnlySpan<byte> message)
    {
        EnsureActive();
        int sent = 0;
        // 流模式不保留 Send 边界 新数据可接在尚未分配序号的尾片后面
        if (streamMode && SendQueueList.TryPeekLast(out var Previous) && Previous.DataList.Length < MaxSegmentSize)
        {
            sent = Math.Min(message.Length, MaxSegmentSize - Previous.DataList.Length);
            if (sent > 0)
            {
                var combinedList = new byte[Previous.DataList.Length + sent];
                Previous.DataList.CopyTo(combinedList, 0);
                message[..sent].CopyTo(combinedList.AsSpan(Previous.DataList.Length));
                SendQueueList.ReplaceLast(CreateSegment(EKcpCommand.Push, 0, 0, 0, combinedList));
                message = message[sent..];
            }
        }
        if (streamMode && message.IsEmpty)
            return sent;

        int messageLength = message.Length;
        // 通过最后一个字节下标 计算分片数量
        int fragmentCount = message.IsEmpty ? 1 : 1 + (messageLength - 1) / MaxSegmentSize;
        // 流模式可能已填入尾片 返回已接受的长度 调用方从剩余位置继续发送
        if (fragmentCount >= DefaultReceiveWindow)
            return sent > 0 ? sent : -2;

        // 按片数循环,将消息分片入队
        for (int index = 0; index < fragmentCount; index++)
        {
            // 比如message.Length = 1377 , length = 1376
            int length = Math.Min(message.Length, MaxSegmentSize);
            // 剩余片数 = 最后一片的下标 - 当前片下标
            byte RemainingFragmentCount = streamMode ? (byte)0 : (byte)(fragmentCount - 1 - index);
            SendQueueList.Enqueue(CreateSegment(EKcpCommand.Push, RemainingFragmentCount,
                0, 0, message[..length].ToArray()));
            message = message[length..];
            sent += length;
        }
        return sent;
    }

    #endregion

    #region 窗口准入与报文输出

    /// <summary>
    /// 在有效发送窗口内为待发分片分配序号
    /// </summary>
    private void FlushNewSegments(uint now, uint window)
    {
        // 遍历队列中的分片
        // 窗口限制序号范围 例如窗口为 2 时即使 1 已确认 只要 0 未确认就不能准入 2
        while (SendQueueList.Count > 0 &&
            Difference(SendNext, unchecked(sendUnacknowledged + window)) < 0)
        {
            var Segment = SendQueueList.Dequeue();
            // 下一个待分配的发送序号
            uint sequence = SendNext;
            SendNext = unchecked(SendNext + 1);
            // 入缓冲只表示已占用窗口 Transmissions 为零的记录尚未首次发出
            Segment = CreateSegment(EKcpCommand.Push, Segment.Header.RemainingFragmentCount,
                now, sequence, Segment.DataList);
            sendBufferDict.Add(sequence,
                new UnacknowledgedSegment(Segment, now, retransmissionTimeout));
        }
    }

    /// <summary>
    /// 发送段
    /// </summary>
    /// <param name="unacknowledged">待输出的完整段及重传状态</param>
    /// <param name="now">当前时间 单位为毫秒</param>
    private void OutputPush(UnacknowledgedSegment unacknowledged, uint now, ref int bufferedLength)
    {
        var Segment = unacknowledged.Segment;
        // PacketSent 回调可能改变窗口和接收进度 输出前重新取得当前值
        unacknowledged.Segment = CreateSegment(EKcpCommand.Push, Segment.Header.RemainingFragmentCount,
            now, Segment.Header.SendSequence, Segment.DataList);
        AppendSegment(ref bufferedLength, unacknowledged.Segment);
    }

    /// <summary>
    /// 用当前会话窗口和累计确认生成段头 保留传入的负载数组
    /// </summary>
    private KcpSegment CreateSegment(EKcpCommand command, byte remainingFragmentCount,
        uint timestamp, uint sequence, byte[] dataList)
    {
        // 每个命令都附带本端最新接收进度 receiveNext 为 3 就同时确认 0 1 2
        var Header = new KcpHeader(ConversationId, command, remainingFragmentCount,
            AvailableReceiveWindow, timestamp, sequence, receiveNext, (uint)dataList.Length);
        return new KcpSegment(Header, dataList);
    }

    private void AppendSegment(ref int bufferedLength, EKcpCommand command, byte remainingFragmentCount,
        uint timestamp, uint sequence, byte[] dataList)
    {
        var Segment = CreateSegment(command, remainingFragmentCount, timestamp, sequence, dataList);
        AppendSegment(ref bufferedLength, Segment);
    }

    private void AppendSegment(ref int bufferedLength, KcpSegment segment)
    {
        int length = KcpHeader.HeadSize + segment.DataList.Length;
        // 一份 UDP 报文可以容纳多个完整段 放不下当前段时先输出之前积累的段
        if (length > Mtu - bufferedLength)
        {
            Emit(bufferedLength);
            bufferedLength = 0;
        }
        var writer = new KcpDatagramWriter(outputBufferList.AsSpan(bufferedLength));
        writer.WriteSegment(segment.Header, segment.DataList);
        bufferedLength += writer.WrittenLength;
    }

    private void Emit(int length)
    {
        // 输出数组交给外层持有 合并缓冲区留给下一份报文复用
        if (length > 0)
            Output(outputBufferList.AsSpan(0, length).ToArray());
    }

    #endregion
}
