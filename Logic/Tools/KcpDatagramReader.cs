using System;
using System.IO;
namespace MmKcp{

    /// <summary>
    /// Kcp数据包读取器,最大的作用就是解析数据包头部 + 移动指针
    /// <para>
    /// KcpDatagramReader 用 ref struct，是因为它要把 ReadOnlySpan<byte> 存成字段
    /// 而 C# 只允许 ref struct 持有这种字段，以防它引用的临时内存已经失效却还能被访问
    /// </para>
    /// </summary>
    internal ref struct KcpDatagramReader
    {
        // 当前会话允许接收的报文Id
        private readonly uint ConversationId;

        // 此数据报没有读过的区间
        private ReadOnlySpan<byte> Remaining;

        public KcpDatagramReader(uint conversationId, ReadOnlySpan<byte> datagram){

            if(datagram.Length < KcpHeader.HeadSize){
                throw new ArgumentException("Kcp数据报没有完成段头");
            }

            ConversationId = conversationId;
            Remaining = datagram;
        }
        
        /// <summary>
        /// 尝试读取Kcp数据报的头部和负载,并更新剩余数据
        /// </summary>
        /// <param name="header"></param>
        /// <param name="payload"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        public bool TryRead(out KcpHeader header, out ReadOnlySpan<byte> payload){
            if(Remaining.IsEmpty){
                header = default;
                payload = default;
                return false;
            }

            if(Remaining.Length < KcpHeader.HeadSize){
                throw new ArgumentException("Kcp数据报末没有完成段头");
            }
            // Remaining = 24头 + 22负载 + 24ack = 70 , header =24
            header = KcpHeader.Read(Remaining);
            // 验证头部
            Validate(header);

            // 22
            int payloadLength = (int) header.PayloadLength;
            // 截取负载[24,24+22]
            payload = Remaining.Slice(KcpHeader.HeadSize, payloadLength);
            // 剩余数据 = 截取后的部分 = 70 - 24 - 22 = 24ack
            Remaining = Remaining[(KcpHeader.HeadSize + payloadLength)..];
            return true;
        }
    

        /// <summary>
        /// 验证Kcp数据报的头部
        /// </summary>
        /// <param name="header"></param>
        /// <exception cref="ArgumentException"></exception>
        private void Validate(KcpHeader header){
            if(header.Conversation != ConversationId){
                throw new ArgumentException("Kcp数据报的会话ID不匹配");
            }

            if(header.Command is < EKcpCommand.Push or > EKcpCommand.WindowTell){
                throw new ArgumentException("Kcp数据报的命令不合法");
            }
            
            if(header.PayloadLength > (uint)Remaining.Length - KcpHeader.HeadSize){
                throw new ArgumentException("Kcp数据报的负载长度不合法");
            }
        }
    } 
}