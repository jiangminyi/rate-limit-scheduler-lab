using System.Diagnostics;

namespace RateScheduler;

public interface IMonotonicClock { long Ticks { get; } }
public sealed class StopwatchClock : IMonotonicClock
{
    private readonly long started = Stopwatch.GetTimestamp();
    public long Ticks => Stopwatch.GetElapsedTime(started).Ticks;
}
public sealed class QueueFullException : Exception;
public sealed record Permit(long Sequence, int Cost, long GrantedAtTicks);
public sealed record Snapshot(int Pending, decimal Tokens, long NextSlotTicks, bool Disposed);

/// <summary>调用 Pump 驱动的有界准入调度器，不执行用户回调，不创建后台线程。</summary>
public sealed class Scheduler : IDisposable
{
    private sealed class Entry(long sequence, int cost, int priority, long enqueued, CancellationToken token)
    {
        public long Sequence { get; } = sequence;
        public int Cost { get; } = cost;
        public int Priority { get; } = priority;
        public long Enqueued { get; } = enqueued;
        public CancellationToken Token { get; } = token;
        public TaskCompletionSource<Permit> Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public CancellationTokenRegistration Registration;
        public bool Pending = true;
    }

    private readonly object gate = new();
    private readonly IMonotonicClock clock;
    private readonly int capacity;
    private readonly decimal tokensPerSecond;
    private readonly int queueCapacity;
    private readonly long spacing;
    private readonly long aging;
    private readonly List<Entry> queue = [];
    private decimal tokens;
    private long lastRefill;
    private long lastObserved;
    private long nextSlot;
    private long sequence;
    private bool disposed;

    public Scheduler(IMonotonicClock clock, int tokenCapacity, decimal tokensPerSecond, int queueCapacity,
        TimeSpan minimumSpacing, TimeSpan agingInterval)
    {
        ArgumentNullException.ThrowIfNull(clock);
        if (tokenCapacity <= 0 || tokensPerSecond < 0.000001m || tokensPerSecond > 1_000_000_000m || queueCapacity <= 0 ||
            minimumSpacing < TimeSpan.Zero || agingInterval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(tokenCapacity), "容量、速率或时间参数超出支持范围。");
        this.clock = clock; capacity = tokenCapacity; this.tokensPerSecond = tokensPerSecond;
        this.queueCapacity = queueCapacity; spacing = minimumSpacing.Ticks; aging = agingInterval.Ticks;
        lastObserved = clock.Ticks;
        if (lastObserved < 0) throw new ArgumentOutOfRangeException(nameof(clock), "时钟必须从非负值开始。");
        lastRefill = lastObserved; tokens = tokenCapacity;
    }

    public Task<Permit> Enqueue(int cost = 1, int priority = 0, CancellationToken cancellationToken = default)
    {
        if (cost <= 0 || cost > capacity || priority is < 0 or > 9)
            throw new ArgumentOutOfRangeException(nameof(cost), "成本需在桶容量内，优先级需在0至9之间。");
        lock (gate)
        {
            ObjectDisposedException.ThrowIf(disposed, this);
            var now = ReadClock();
            if (cancellationToken.IsCancellationRequested) return Task.FromCanceled<Permit>(cancellationToken);
            if (queue.Count == queueCapacity) throw new QueueFullException();
            var entry = new Entry(checked(++sequence), cost, priority, now, cancellationToken);
            queue.Add(entry);
            // Register 可能同步调用；Monitor 可重入。注册发布后再次检查，覆盖取消先于赋值的竞态。
            entry.Registration = cancellationToken.Register(() => Cancel(entry));
            if (!entry.Pending) entry.Registration.Unregister();
            return entry.Completion.Task;
        }
    }

    public int Pump(int maxGrants = int.MaxValue)
    {
        if (maxGrants <= 0) throw new ArgumentOutOfRangeException(nameof(maxGrants));
        lock (gate)
        {
            ObjectDisposedException.ThrowIf(disposed, this);
            var now = ReadClock();
            Refill(now);
            var granted = 0;
            while (queue.Count > 0 && granted < maxGrants && now >= nextSlot)
            {
                var entry = queue.OrderByDescending(e => EffectivePriority(e, now)).ThenBy(e => e.Sequence).First();
                // CancellationToken 的状态可能已更新，但回调仍在等待本锁。
                if (entry.Token.IsCancellationRequested) { Cancel(entry); continue; }
                if (tokens < entry.Cost) break; // 严格优先顺序：不会用便宜请求绕过队首。
                var next = checked(now + spacing); // 先检查溢出，再消耗令牌或完成任务。
                tokens -= entry.Cost;
                nextSlot = next;
                entry.Pending = false; queue.Remove(entry); entry.Registration.Unregister();
                entry.Completion.TrySetResult(new(entry.Sequence, entry.Cost, now));
                granted++;
            }
            return granted;
        }
    }

    // 仅观察已处理状态，不隐式推进时钟或补充令牌。
    public Snapshot Inspect() { lock (gate) return new(queue.Count, tokens, nextSlot, disposed); }

    public void Dispose()
    {
        lock (gate)
        {
            if (disposed) return;
            disposed = true;
            foreach (var entry in queue)
            {
                entry.Pending = false; entry.Registration.Unregister();
                entry.Completion.TrySetException(new ObjectDisposedException(nameof(Scheduler)));
            }
            queue.Clear();
        }
    }

    private int EffectivePriority(Entry entry, long now) => (int)Math.Min(9L, entry.Priority + Math.Min(9L, (now - entry.Enqueued) / aging));
    private long ReadClock()
    {
        var now = clock.Ticks;
        if (now < lastObserved) throw new InvalidOperationException("时钟倒退；本次操作未更新调度状态。");
        lastObserved = now;
        return now;
    }
    private void Refill(long now)
    {
        var elapsedSeconds = (now - lastRefill) / (decimal)TimeSpan.TicksPerSecond;
        tokens = Math.Min(capacity, tokens + elapsedSeconds * tokensPerSecond);
        lastRefill = now;
    }
    private void Cancel(Entry entry)
    {
        lock (gate)
        {
            if (!entry.Pending) return;
            entry.Pending = false; queue.Remove(entry); entry.Registration.Unregister();
            entry.Completion.TrySetCanceled(entry.Token);
        }
    }
}
