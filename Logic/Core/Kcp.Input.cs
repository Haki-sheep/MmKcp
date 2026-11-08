using System;
using System.Collections.Generic;

namespace MmKcp;

/// <remarks>
/// 解析报文并分派命令 接收段去重排序和业务消息重组
/// </remarks>
public sealed partial class Kcp
{
    /// <summary> 等待缺口补齐的段 以发送序号为键 </summary>
    private readonly Dictionary<uint, KcpSegment> receiveBufferDict = new();
    /// <summary> 已按序排好且等待业务读取的段 </summary>
    private readonly Queue<KcpSegment> receiveQueueList = new();
    /// <summary> 下一个期待收到的段序号 </summary>
    private uint receiveNext;

    #region 接收与重组

    /// <summary>
    /// 接收已校验的 Push 段 将连续收到的段按序入队
    /// </summary>
    /// <param name="header">头部数据</param>
    /// <param name="payload">负载数据</param>
    private void InputPush(KcpHeader header, ReadOnlySpan<byte> payload)
    {
        uint sequence = header.SendSequence;
        // 用差值的符号区分回绕前后的旧段和新段
        int offset = Difference(sequence, receiveNext);
        if (offset >= receiveWindow)
            return;

        // 加入确认列表,重复段也要回,因为上一次确认可能丢了
        pendingAckList.Add((sequence, header.Timestamp));
        if (offset < 0 || receiveBufferDict.ContainsKey(sequence))
            return;
        receiveBufferDict.Add(sequence,
            new KcpSegment(header, payload.ToArray()));
        MoveContiguousSegments();
    }

    private void MoveContiguousSegments()
    {
        // 将期望的段移出字典,加入队列
        // 比如012发出,102收到,1先存着
        // 下一次InputPush , 0来了以后立马出字典入队,receiveNext +1 = 1
        // 1出字典入队,receiveNext +1 = 2
        // 2到达后再出字典入队 receiveNext +1 = 3
        // 队列满时连续段也先留在字典 业务读取腾出位置后再推进 receiveNext
        while (receiveQueueList.Count < receiveWindow &&
            receiveBufferDict.TryGetValue(receiveNext, out var Segment))
        {
            receiveBufferDict.Remove(receiveNext);
            receiveQueueList.Enqueue(Segment);
            receiveNext = unchecked(receiveNext + 1);
        }
    }

    /// <summary>
    /// 读取一个 UDP 数据报里的所有 KCP 命令 使用当前协议时间
    /// </summary>
    public void Input(ReadOnlySpan<byte> datagram) => Input(datagram, currentTime);

    /// <summary>
    /// 使用与 Update 同源的单调毫秒计数处理收到的报文
    /// </summary>
    public void Input(ReadOnlySpan<byte> datagram, uint now)
    {
        EnsureActive();
        // 完整检查后再更新会话 避免合并报文只应用了有效的前半部分
        var validationReader = new KcpDatagramReader(ConversationId, datagram);
        while (validationReader.TryRead(out _, out _)) { }

        currentTime = now;
        uint previousUna = sendUnacknowledged;
        uint previousInFlight = (uint)sendBufferDict.Count;
        uint acknowledgedBytes = 0;
        var reader = new KcpDatagramReader(ConversationId, datagram);
        while (reader.TryRead(out var header, out var payload))
        {
            // 累计确认可能先移除本次 ACK 对应的记录 先保存它才能继续测量 RTT
            UnacknowledgedSegment? confirmed = null;
            if (header.Command == EKcpCommand.Ack)
                sendBufferDict.TryGetValue(header.SendSequence, out confirmed);
            // 每种命令都携带窗口和累计确认 即使单独的 ACK 丢失也能推进发送状态
            remoteWindow = header.Window;
            acknowledgedBytes += ApplyCumulativeAck(header.receiveNext);
            switch (header.Command)
            {
                case EKcpCommand.Ack:
                    acknowledgedBytes += InputAck(header, now, confirmed);
                    break;
                case EKcpCommand.Push:
                    // 一条消息的头和负载都读取完毕,开始处理
                    InputPush(header, payload);
                    break;
                case EKcpCommand.WindowAsk:
                    tellWindow = true;
                    break;
                case EKcpCommand.WindowTell:
                    break;
            }
        }
        OnAcknowledged(previousUna, acknowledgedBytes, previousInFlight);
    }

    /// <summary>
    /// 返回下一条完整消息的字节数 尚未收齐时返回负一
    /// </summary>
    public int PeekSize()
    {
        EnsureActive();
        if (!receiveQueueList.TryPeek(out var first))
            return -1;
        // 需要收齐的片数,自己也算一片
        int fragmentCount = first.Header.RemainingFragmentCount + 1;
        if (receiveQueueList.Count < fragmentCount)
            return -1;
        int messageLength = 0;
        int count = 0;
        // 遍历队列,累加负载长度,直到收齐为止
        foreach (var Segment in receiveQueueList)
        {
            messageLength += Segment.DataList.Length;
            count++;
            if (count == fragmentCount)
                break;
        }
        return messageLength;
    }

    /// <summary>
    /// 取出一条完整消息 未收齐时返回 null
    /// </summary>
    public byte[]? Recv()
    {
        int length = PeekSize();
        if (length < 0)
            return null;
        // 消息列表
        var messageList = new byte[length];
        Recv(messageList.AsSpan());
        return messageList;
    }

    /// <summary>
    /// 读取完整消息 返回字节数 空队列返回负一 未收齐返回负二 目标过小返回负三
    /// </summary>
    public int Recv(Span<byte> destination)
    {
        EnsureActive();
        if (!receiveQueueList.TryPeek(out var first))
            return -1;
        int messageLength = PeekSize();
        if (messageLength < 0)
            return -2;
        // 在任何分片出队前检查容量 失败时整条消息仍留在队列
        if (messageLength > destination.Length)
            return -3;
        int fragmentCount = first.Header.RemainingFragmentCount + 1;
        bool wasFull = receiveQueueList.Count >= receiveWindow;
        int offset = 0;
        // 遍历队列,将负载数据复制到消息列表中
        for (int index = 0; index < fragmentCount; index++)
        {
            var Segment = receiveQueueList.Dequeue();
            Segment.DataList.AsSpan().CopyTo(destination[offset..]);
            offset += Segment.DataList.Length;
        }
        MoveContiguousSegments();
        // 必须在补入已缓存段后仍有空位才通告窗口恢复 避免刚读出又被填满
        if (wasFull && receiveQueueList.Count < receiveWindow)
            tellWindow = true;
        return messageLength;
    }

    #endregion
}
