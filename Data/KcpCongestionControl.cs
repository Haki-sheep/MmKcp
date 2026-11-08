using System;

namespace MmKcp;

/// <summary>
/// 自定义发送拥塞策略 根据确认和丢包情况调整 CongestionWindow 单位为段
/// </summary>
/// <remarks>
/// 通过 SetCongestionControl 安装 所有回调的第一个参数都是当前 Kcp 会话
/// 未提供 Acknowledged FastRetransmit 或 Timeout 时 对应调整仍使用内置算法
/// 实际发送窗口还受本端发送窗口和对方接收窗口限制 例如三者为 32 128 4 时最多使用 4 段
/// 每个策略实例由一个会话独占 回调同步执行 可修改 CongestionWindow 但不得重入当前会话
/// </remarks>
public sealed class KcpCongestionControl
{
    /// <summary> 安装新策略时调用 可设置初始拥塞窗口 </summary>
    public Action<Kcp>? Initialize { get; set; }

    /// <summary> 策略被替换或会话释放时调用 用于释放策略持有的资源 </summary>
    public Action<Kcp>? Release { get; set; }

    /// <summary> 一份输入报文使最早未确认序号前移后调用 用于增加拥塞窗口 </summary>
    /// <remarks>
    /// 会话后的参数依次为序号推进量 本次新确认的负载字节数 处理报文前的发送缓冲段数
    /// 例如 1 已单独确认 本次确认 0 后序号可从 0 前移到 2 推进量为 2 但新增确认数据只有段 0
    /// </remarks>
    public Action<Kcp, uint, uint, uint>? Acknowledged { get; set; }

    /// <summary> 本轮存在快速重传时在输出后调用 用于降低拥塞窗口 </summary>
    /// <remarks> 会话后的参数依次为本轮快速重传段数 本轮发送缓冲段数 本轮发送前的拥塞窗口 </remarks>
    public Action<Kcp, uint, uint, uint>? FastRetransmit { get; set; }

    /// <summary> 本轮存在超时重传时在输出后调用 会话后的参数为本轮发送前的拥塞窗口 </summary>
    /// <remarks> 同轮同时发生快速重传和超时重传时 先调用 FastRetransmit 再调用 Timeout </remarks>
    public Action<Kcp, uint>? Timeout { get; set; }

    /// <summary> 每次实际 Flush 开始时调用 未到刷新时间的 Update 不触发 </summary>
    public Action<Kcp>? Tick { get; set; }

    /// <summary> 待发队列已空且有效窗口仍有余量时调用 表示业务数据未填满窗口 </summary>
    /// <remarks> 会话后的参数为发送缓冲段数 其中可能包含因发送预算不足而尚未首次发出的段 </remarks>
    public Action<Kcp, uint>? AppLimited { get; set; }

    /// <summary> 更新往返耗时估算后调用 会话后的参数为当前 ACK 回显时间测得的往返毫秒数 </summary>
    public Action<Kcp, int>? RttMeasured { get; set; }

    /// <summary> 数据段编码前调用 首次发送和重传都会触发 </summary>
    /// <remarks> 会话后的参数依次为段序号 本次发送的毫秒时间戳 负载字节数 本轮发送缓冲段数 累计发送次数 首次次数为 1 </remarks>
    public Action<Kcp, uint, uint, int, uint, uint>? PacketSent { get; set; }

    /// <summary> 单段确认或累计确认移除一个发送缓冲记录后调用 </summary>
    /// <remarks>
    /// 会话后的参数依次为段序号 最近发送的毫秒时间戳 负载字节数 距最近发送的毫秒数 累计发送次数
    /// 累计确认无法确定对应哪次发送 耗时为负一 ACK 回显旧发送时 此处耗时与 RttMeasured 的样本可能不同
    /// </remarks>
    public Action<Kcp, uint, uint, int, int, uint>? PacketAcknowledged { get; set; }

    /// <summary> 每轮 Tick 后取得数据发送预算 单位为负载字节 负值表示不限速 </summary>
    /// <remarks> 首次发送和重传共用预算 不足一整段时暂停数据发送 ACK 和窗口通告不消耗预算 </remarks>
    public Func<Kcp, int>? GetPacingBudget { get; set; }
}
