namespace MmKcp;

/// <summary>
/// 完整协议段由段头和负载组成 待发段进入发送窗口时分配序号
/// </summary>
internal readonly struct KcpSegment
{
    public KcpHeader Header { get; }
    // 负载数据列表
    public byte[] DataList { get; }

    /// <summary>
    /// 保存段头并持有负载数组 不复制负载内容
    /// </summary>
    public KcpSegment(KcpHeader header, byte[] dataList)
    {
        Header = header;
        DataList = dataList;
    }
}
