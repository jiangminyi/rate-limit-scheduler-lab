using RateScheduler;
var clock = new DemoClock();
using var scheduler = new Scheduler(clock, 3, 2, 20, TimeSpan.FromMilliseconds(250), TimeSpan.FromSeconds(1));
var pending = new[] { scheduler.Enqueue(2, 0), scheduler.Enqueue(1, 8), scheduler.Enqueue(1, 3) };
while (pending.Any(t => !t.IsCompleted))
{
    var granted = scheduler.Pump();
    Console.WriteLine($"t={clock.Ticks / (double)TimeSpan.TicksPerSecond:F2}s，授予={granted}，剩余令牌={scheduler.Inspect().Tokens:F2}");
    clock.Ticks += TimeSpan.FromMilliseconds(250).Ticks;
}
foreach (var task in pending) Console.WriteLine($"请求 {task.Result.Sequence}，成本 {task.Result.Cost}，授予时间 {task.Result.GrantedAtTicks / (double)TimeSpan.TicksPerSecond:F2}s");
sealed class DemoClock : IMonotonicClock { public long Ticks { get; set; } }
