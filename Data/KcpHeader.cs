using System;
using System.Buffers.Binary;

namespace MmKcp
{

    /// <summary>
    /// Kcp命令类型
    /// </summary>
    public enum EKcpCommand : byte
    {
        // 数据段
        Push = 81,
        // 确认段
        Ack = 82,
        // 询问对端接收窗口
        WindowAsk = 83,
        // 通告本端接收窗口
        WindowTell = 84
    }

    /// <summary>
    /// KCP 段的固定24字节协议头
    /// </summary>
    public readonly struct KcpHeader
    {
        // 会话ID
        public uint Conversation { get; }
        // 命令
        public EKcpCommand Command { get; }
        // 本消息后续还有几片 不含当前片
        public byte RemainingFragmentCount { get; }
        // 接收窗口大小
        public ushort Window { get; }
        // 发送时间戳
        public uint Timestamp { get; }
        // 发送序列号
        public uint SendSequence { get; }
        // KCP 头的 una 字段 表示发包方下一个期待收到的段序号
        public uint receiveNext { get; }
        // 本段负载字节数
        public uint PayloadLength { get; }

        // 头部大小
        public const int HeadSize = 24;

        public KcpHeader(uint conversation,
                         EKcpCommand command,
                         byte remainingFragmentCount,
                         ushort window,
                         uint timestamp,
                         uint sendSequence,
                         uint receiveNext,
                         uint length)
        {
            Conversation = conversation;
            Command = command;
            RemainingFragmentCount = remainingFragmentCount;
            Window = window;
            Timestamp = timestamp;
            SendSequence = sendSequence;
            this.receiveNext = receiveNext;
            PayloadLength = length;
        }

        /// <summary>
        /// 将报文开头24字节解释为一个KCP段头
        /// </summary>
        public static KcpHeader Read(ReadOnlySpan<byte> source)
        {
            uint conversation = BinaryPrimitives.ReadUInt32LittleEndian(source);
            var eCommand = (EKcpCommand)source[4];
            byte RemainingFragmentCount = source[5];
            ushort window = BinaryPrimitives.ReadUInt16LittleEndian(source[6..]);
            uint timestamp = BinaryPrimitives.ReadUInt32LittleEndian(source[8..]);
            uint sendSequence = BinaryPrimitives.ReadUInt32LittleEndian(source[12..]);
            uint receiveNext = BinaryPrimitives.ReadUInt32LittleEndian(source[16..]);
            uint length = BinaryPrimitives.ReadUInt32LittleEndian(source[20..]);

            return new KcpHeader(
                conversation,
                eCommand,
                RemainingFragmentCount,
                window,
                timestamp,
                sendSequence,
                receiveNext,
                length);
        }

        /// <summary>
        /// 将本段头写到目标缓冲区
        /// </summary>
        public void Write(Span<byte> destination)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(destination, this.Conversation);
            destination[4] = (byte)this.Command;
            destination[5] = this.RemainingFragmentCount;
            BinaryPrimitives.WriteUInt16LittleEndian(destination[6..], this.Window);
            BinaryPrimitives.WriteUInt32LittleEndian(destination[8..], this.Timestamp);
            BinaryPrimitives.WriteUInt32LittleEndian(destination[12..], this.SendSequence);
            BinaryPrimitives.WriteUInt32LittleEndian(destination[16..], this.receiveNext);
            BinaryPrimitives.WriteUInt32LittleEndian(destination[20..], this.PayloadLength);
        }
    }
}

