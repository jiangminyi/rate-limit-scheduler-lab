using RateScheduler;

var specs = new (string, Action)[]
{
    ("初始突发受令牌容量限制", () => {
        var clock = new FakeClock(); using var s = Make(clock, 3); var tasks = Enumerable.Range(0, 5).Select(_ => s.Enqueue()).ToArray();
        Equal(3, s.Pump()); Equal(3, tasks.Count(t => t.IsCompletedSuccessfully)); Equal(0m, s.Inspect().Tokens);
    }),
    ("小数补充在精确边界放行", () => {
        var clock = new FakeClock(); using var s = Make(clock, 1, rate: 0.5m); s.Enqueue(); s.Pump(); var next = s.Enqueue();
        clock.Set(19_999_999); Equal(0, s.Pump()); Equal(false, next.IsCompleted); clock.Set(20_000_000); Equal(1, s.Pump());
    }),
    ("长时间空闲不突破桶容量", () => {
        var clock = new FakeClock(); using var s = Make(clock, 2); s.Enqueue(2); s.Pump(); clock.Set(long.MaxValue - 1);
        s.Enqueue(2); s.Enqueue(); Equal(1, s.Pump()); Equal(0m, s.Inspect().Tokens);
    }),
    ("漏桶间隔即使令牌充足也阻止突发", () => {
        var clock = new FakeClock(); using var s = Make(clock, 10, spacing: TimeSpan.FromSeconds(1)); s.Enqueue(); s.Enqueue(); s.Enqueue();
        Equal(1, s.Pump()); Equal(0, s.Pump()); clock.Set(10_000_000); Equal(1, s.Pump()); clock.Set(20_000_000); Equal(1, s.Pump());
    }),
    ("空闲后漏桶不积攒补偿突发", () => {
        var clock = new FakeClock(); using var s = Make(clock, 10, spacing: TimeSpan.FromSeconds(1)); s.Enqueue(); s.Pump();
        clock.Set(1_000_000_000); s.Enqueue(); s.Enqueue(); Equal(1, s.Pump()); Equal(0, s.Pump());
    }),
    ("高优先级优先且同级FIFO", () => {
        var clock = new FakeClock(); using var s = Make(clock, 1); var low = s.Enqueue(priority: 1); var high1 = s.Enqueue(priority: 8); var high2 = s.Enqueue(priority: 8);
        s.Pump(); Equal(true, high1.IsCompletedSuccessfully); Equal(false, high2.IsCompleted); Equal(false, low.IsCompleted);
        clock.Set(10_000_000); s.Pump(); Equal(true, high2.IsCompletedSuccessfully);
    }),
    ("老化使旧低优先请求达到最高级并先获服务", () => {
        var clock = new FakeClock(); using var s = Make(clock, 1); s.Enqueue(); s.Pump(); var old = s.Enqueue(priority: 0);
        for (var second = 1; second <= 8; second++) { clock.Set(second * 10_000_000L); var fresh = s.Enqueue(priority: 9); s.Pump(); Equal(true, fresh.IsCompletedSuccessfully); }
        clock.Set(90_000_000); var last = s.Enqueue(priority: 9); s.Pump(); Equal(true, old.IsCompletedSuccessfully); Equal(false, last.IsCompleted);
    }),
    ("昂贵队首不会被便宜请求绕过", () => {
        var clock = new FakeClock(); using var s = Make(clock, 3); s.Enqueue(2); s.Pump(); var expensive = s.Enqueue(3, 9); var cheap = s.Enqueue(1, 0);
        Equal(0, s.Pump()); Equal(false, cheap.IsCompleted); clock.Set(20_000_000); Equal(1, s.Pump()); Equal(true, expensive.IsCompletedSuccessfully);
    }),
    ("取消立即移除排队项且不消耗令牌", () => {
        var clock = new FakeClock(); using var s = Make(clock, 2); using var cancel = new CancellationTokenSource(); var task = s.Enqueue(2, cancellationToken: cancel.Token);
        cancel.Cancel(); Equal(true, task.IsCanceled); Equal(0, s.Inspect().Pending); Equal(2m, s.Inspect().Tokens);
    }),
    ("已取消请求不占满队列", () => {
        var clock = new FakeClock(); using var s = Make(clock, 1, queue: 1); using var cancel = new CancellationTokenSource(); cancel.Cancel();
        Equal(true, s.Enqueue(cancellationToken: cancel.Token).IsCanceled); s.Enqueue(); Equal(1, s.Inspect().Pending);
    }),
    ("放行后取消不撤销已授予额度", () => {
        var clock = new FakeClock(); using var s = Make(clock, 1); using var cancel = new CancellationTokenSource(); var task = s.Enqueue(cancellationToken: cancel.Token);
        s.Pump(); cancel.Cancel(); Equal(true, task.IsCompletedSuccessfully); Equal(0m, s.Inspect().Tokens);
    }),
    ("有界排队拒绝后状态不变", () => {
        var clock = new FakeClock(); using var s = Make(clock, 1, queue: 1); s.Enqueue(); Throws<QueueFullException>(() => s.Enqueue());
        Equal(1, s.Inspect().Pending); Equal(1m, s.Inspect().Tokens);
    }),
    ("倒退时钟失败且可恢复", () => {
        var clock = new FakeClock(); using var s = Make(clock, 1); clock.Set(10); s.Enqueue(); var before = s.Inspect();
        clock.Set(9); Throws<InvalidOperationException>(() => s.Pump()); Equal(before, s.Inspect()); clock.Set(10); Equal(1, s.Pump());
    }),
    ("时间上界溢出不会消费额度或完成请求", () => {
        var clock = new FakeClock(); using var s = Make(clock, 1, spacing: TimeSpan.FromTicks(2)); clock.Set(long.MaxValue); var task = s.Enqueue();
        Throws<OverflowException>(() => s.Pump()); Equal(1m, s.Inspect().Tokens); Equal(1, s.Inspect().Pending); Equal(false, task.IsCompleted);
    }),
    ("Dispose释放队列且后续操作失败", () => {
        var clock = new FakeClock(); var s = Make(clock, 1); var task = s.Enqueue(); s.Dispose(); s.Dispose();
        Throws<ObjectDisposedException>(() => task.GetAwaiter().GetResult()); Equal(0, s.Inspect().Pending);
        Throws<ObjectDisposedException>(() => s.Enqueue()); Throws<ObjectDisposedException>(() => s.Pump());
    }),
    ("配置与不可能满足的成本立即拒绝", () => {
        var clock = new FakeClock(); Throws<ArgumentOutOfRangeException>(() => Make(clock, 0)); Throws<ArgumentOutOfRangeException>(() => Make(clock, 1, rate: 0));
        using var s = Make(clock, 1); Throws<ArgumentOutOfRangeException>(() => s.Enqueue(2)); Throws<ArgumentOutOfRangeException>(() => s.Enqueue(0));
        Throws<ArgumentOutOfRangeException>(() => s.Enqueue(priority: 10)); Throws<ArgumentOutOfRangeException>(() => s.Pump(0));
    }),
    ("每次Pump预算限制最多授予数量", () => {
        var clock = new FakeClock(); using var s = Make(clock, 5); for (var i = 0; i < 5; i++) s.Enqueue();
        Equal(2, s.Pump(2)); Equal(3, s.Inspect().Pending); Equal(3, s.Pump());
    }),
    ("并发入队与Pump不会超额或重复授予", () => {
        var clock = new FakeClock(); using var s = Make(clock, 100, queue: 1000); var tasks = new Task<Permit>[500];
        Parallel.For(0, 500, i => tasks[i] = s.Enqueue()); Parallel.For(0, 20, _ => s.Pump());
        Equal(100, tasks.Count(t => t.IsCompletedSuccessfully)); Equal(100, tasks.Where(t => t.IsCompletedSuccessfully).Select(t => t.Result.Sequence).Distinct().Count());
        Equal(0m, s.Inspect().Tokens); Equal(400, s.Inspect().Pending);
    }),
    ("取消与授予竞态每轮满足额度守恒", () => {
        for (var round = 0; round < 200; round++) {
            var clock = new FakeClock(); using var s = Make(clock, 1); using var cancel = new CancellationTokenSource(); var task = s.Enqueue(cancellationToken: cancel.Token);
            Parallel.Invoke(() => cancel.Cancel(), () => s.Pump());
            Equal(true, task.IsCompleted); Equal(0, s.Inspect().Pending);
            Equal(task.IsCanceled ? 1m : 0m, s.Inspect().Tokens);
        }
    }),
    ("独立离散令牌模型校验2000步混合轨迹", () => {
        var clock = new FakeClock(); using var s = Make(clock, 7, rate: 2); var random = new Random(20261030);
        // 参考模型采用半秒整数步长，令牌以整数演进，与实现的decimal秒补充路径独立。
        var pending = new Queue<(int cost, Task<Permit> task)>(); var referenceTokens = 7; long tick = 0;
        for (var i = 0; i < 2000; i++) {
            var elapsedSteps = random.Next(0, 4); tick += elapsedSteps * 5_000_000L; clock.Set(tick); referenceTokens = Math.Min(7, referenceTokens + elapsedSteps);
            if (random.Next(3) != 0) { var cost = random.Next(1, 8); pending.Enqueue((cost, s.Enqueue(cost))); }
            var expected = 0;
            while (pending.TryPeek(out var head) && referenceTokens >= head.cost) { referenceTokens -= head.cost; pending.Dequeue(); expected++; }
            Equal(expected, s.Pump()); Equal((decimal)referenceTokens, s.Inspect().Tokens); Equal(pending.Count, s.Inspect().Pending);
            foreach (var queued in pending) Equal(false, queued.task.IsCompleted);
        }
    }),
    ("漏桶时间戳满足全程最小间隔", () => {
        var clock = new FakeClock(); using var s = Make(clock, 1000, spacing: TimeSpan.FromTicks(7), queue: 1000);
        var tasks = Enumerable.Range(0, 100).Select(_ => s.Enqueue()).ToArray(); var random = new Random(42); long tick = 0;
        while (!tasks.All(t => t.IsCompleted)) { tick += random.Next(0, 20); clock.Set(tick); s.Pump(); }
        var stamps = tasks.Select(t => t.Result.GrantedAtTicks).ToArray();
        for (var i = 1; i < stamps.Length; i++) Equal(true, stamps[i] - stamps[i - 1] >= 7);
    })
};
var failures = 0;
foreach (var (name, test) in specs) { try { test(); Console.WriteLine($"PASS {name}"); } catch (Exception ex) { failures++; Console.Error.WriteLine($"FAIL {name}: {ex}"); } }
Console.WriteLine($"{specs.Length - failures}/{specs.Length} 规格通过");
return failures == 0 ? 0 : 1;

static Scheduler Make(FakeClock clock, int capacity, decimal rate = 1, int queue = 10000, TimeSpan? spacing = null) => new(clock, capacity, rate, queue, spacing ?? TimeSpan.Zero, TimeSpan.FromSeconds(1));
static void Equal<T>(T expected, T actual) { if (!EqualityComparer<T>.Default.Equals(expected, actual)) throw new Exception($"期望 {expected}，实际 {actual}"); }
static void Throws<T>(Action action) where T : Exception { try { action(); } catch (T) { return; } throw new Exception($"未抛出 {typeof(T).Name}"); }
sealed class FakeClock : IMonotonicClock { private long ticks; public long Ticks => Interlocked.Read(ref ticks); public void Set(long value) => Interlocked.Exchange(ref ticks, value); }
