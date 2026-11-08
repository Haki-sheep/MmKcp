namespace MmKcp;

/// <summary> 
/// 已发出且等待确认的段 
/// </summary>
internal sealed class UnacknowledgedSegment
{
    public KcpSegment Segment { get; set; }
    public uint ResendAt { get; set; }
    public uint Timeout { get; set; }
    public uint FastAck { get; set; }
    public uint Transmissions { get; set; }

    /// <param name="segment">已分配发送序号的完整段</param>
    /// <param name="now">当前时间 单位为毫秒</param>
    /// <param name="timeout">重传等待时间 单位为毫秒</param>
    public UnacknowledgedSegment(KcpSegment segment, uint now, uint timeout)
    {
        this.Segment = segment;
        this.Timeout = timeout;
        this.ResendAt = unchecked(now + timeout);
    }
}

