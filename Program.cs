
using System;
using System.Collections.Generic;
using MmKcp;
// dotnet run --project .\Demo\MmKcpDemo.csproj

// 创建实例和接收消息的列表
var datagramList = new List<ReadOnlyMemory<byte>>();
var sender = new Kcp(5, datagramList.Add);
// 默认关闭内置拥塞限制 让三片在首次刷新一起发出
var messageList = new byte[3500];
// 发送消息
sender.Send(messageList);
sender.Update(0);

var receiver = new Kcp(5, _ => { });
foreach (int index in new[] { 2, 0, 2, 1 })
{
    receiver.Input(datagramList[index].Span);
    Console.WriteLine($"投递完序号 {index}");
}

// 读取每个段的头部
foreach (var datagram in datagramList)
{
    var header = KcpHeader.Read(datagram.Span);
    Console.WriteLine(
        $"序号 {header.SendSequence}  RemainingFragmentCount {header.RemainingFragmentCount}  " +
        $"负载 {header.PayloadLength} 字节  整段 {datagram.Length} 字节");

}

