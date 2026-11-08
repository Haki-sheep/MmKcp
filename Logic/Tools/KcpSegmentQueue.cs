using System;

namespace MmKcp;

internal sealed class KcpSegmentQueue
{
    /// <summary> 空闲槽位不持有负载引用 容量保留至 Clear </summary>
    private KcpSegment[] segmentList = Array.Empty<KcpSegment>();
    /// <summary> 队首在数组中的下标 </summary>
    private int head;

    public int Count { get; private set; }

    /// <summary>
    /// 追加待发段 容量不足时按逻辑顺序复制到更大的数组
    /// </summary>
    public void Enqueue(KcpSegment segment)
    {
        if (Count == segmentList.Length)
            Grow();
        int Index = GetIndex(Count);
        segmentList[Index] = segment;
        Count++;
    }

    /// <summary>
    /// 移出队首并清除槽位持有的负载引用
    /// </summary>
    public KcpSegment Dequeue()
    {
        if (Count == 0)
            throw new InvalidOperationException("待发段队列为空");
        var Segment = segmentList[head];
        segmentList[head] = default;
        head++;
        if (head == segmentList.Length)
            head = 0;
        Count--;
        return Segment;
    }

    /// <summary>
    /// 读取尚未分配序号的尾段供流模式合并
    /// </summary>
    public bool TryPeekLast(out KcpSegment segment)
    {
        if (Count == 0)
        {
            segment = default;
            return false;
        }
        segment = segmentList[GetIndex(Count - 1)];
        return true;
    }

    /// <summary>
    /// 替换流模式合并后的尾段 不改变队列顺序
    /// </summary>
    public void ReplaceLast(KcpSegment segment)
    {
        if (Count == 0)
            throw new InvalidOperationException("待发段队列为空");
        segmentList[GetIndex(Count - 1)] = segment;
    }

    /// <summary>
    /// 清空待发段并释放持有的数组
    /// </summary>
    public void Clear()
    {
        segmentList = Array.Empty<KcpSegment>();
        head = 0;
        Count = 0;
    }

    /// <summary>
    /// 将相对队首的位置转换为数组下标
    /// </summary>
    private int GetIndex(int offset)
    {
        int UntilEnd = segmentList.Length - head;
        return offset < UntilEnd ? head + offset : offset - UntilEnd;
    }

    /// <summary>
    /// 将跨越数组末尾的两段数据按顺序搬入新数组
    /// </summary>
    private void Grow()
    {
        int Capacity = segmentList.Length == 0 ? 4 : checked(segmentList.Length * 2);
        var ExpandedList = new KcpSegment[Capacity];
        int FirstCount = Math.Min(Count, segmentList.Length - head);
        Array.Copy(segmentList, head, ExpandedList, 0, FirstCount);
        Array.Copy(segmentList, 0, ExpandedList, FirstCount, Count - FirstCount);
        segmentList = ExpandedList;
        head = 0;
    }
}
