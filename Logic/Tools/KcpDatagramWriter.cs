using System;

namespace MmKcp;

/// <summary>
/// 向调用方提供的缓冲区追加 KCP 段 有效字节数由 WrittenLength 表示
/// </summary>
internal ref struct KcpDatagramWriter
{
    /// <summary> 调用方持有的输出缓冲区 </summary>
    private readonly Span<byte> Buffer;

    public int WrittenLength { get; private set; }

    public KcpDatagramWriter(Span<byte> buffer)
    {
        this.Buffer = buffer;
        WrittenLength = 0;
    }

    /// <summary>
    /// 追加协议头和负载 长度不匹配或空间不足时抛出异常
    /// </summary>
    public void WriteSegment(KcpHeader header, ReadOnlySpan<byte> payload)
    {
        if (header.PayloadLength != (uint)payload.Length)
        {
            throw new ArgumentException(
                $"段头声明 {header.PayloadLength} 字节负载 实际提供 {payload.Length} 字节",
                nameof(payload));
        }

        int available = Buffer.Length - WrittenLength;
        if (available < KcpHeader.HeadSize || payload.Length > available - KcpHeader.HeadSize)
        {
            throw new ArgumentException(
                $"缓冲区剩余 {available} 字节 需要 {KcpHeader.HeadSize} 字节头和 {payload.Length} 字节负载",
                nameof(Buffer));
        }

        var destination = Buffer[WrittenLength..];
        // 先复制负载 允许源数据与输出缓冲区重叠
        payload.CopyTo(destination[KcpHeader.HeadSize..]);
        header.Write(destination);
        WrittenLength += KcpHeader.HeadSize + payload.Length;
    }
}
