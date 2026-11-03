using System.Threading.Channels;
using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Sdk.Test;

namespace Steelax.Pufflow.Operators.Tests.Aggregators;

/// <summary>
///     Black-box tests for the <c>PeriodicRunner</c> operator: values pass through stripped of their
///     watermarks, the periodic handler runs once per interval, the held watermark is released as a bare
///     progress carrier (heartbeat) after each handler invocation, and the last held watermark is released
///     when the source completes. Time is driven by a manual <see cref="ManualTimeProvider" /> so the tests
///     are deterministic (no real wall-clock timing).
/// </summary>
public sealed class PeriodicRunnerProcessorTests
{
    /// <summary>A <see cref="TimeProvider" /> whose timers only fire when the test calls <see cref="Advance" />.</summary>
    private sealed class ManualTimeProvider : TimeProvider
    {
        private readonly object _gate = new();
        private readonly List<ManualTimer> _timers = new();

        public DateTimeOffset Timestamp { get; private set; } = DateTimeOffset.UnixEpoch;

        public override DateTimeOffset GetUtcNow()
        {
            lock (_gate) return Timestamp;
        }

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            var timer = new ManualTimer(this, callback, state);

            lock (_gate)
            {
                timer.Set(dueTime, period);
                _timers.Add(timer);
            }

            return timer;
        }

        /// <summary>Advances the clock and fires any timers that came due on the way.</summary>
        public void Advance(TimeSpan amount)
        {
            ManualTimer[] snapshot;

            lock (_gate)
            {
                Timestamp += amount;
                snapshot = _timers.ToArray();
            }

            var now = Timestamp;

            foreach (var timer in snapshot)
                timer.Fire(now);
        }

        private void Unregister(ManualTimer timer)
        {
            lock (_gate) _timers.Remove(timer);
        }

        private sealed class ManualTimer(ManualTimeProvider owner, TimerCallback callback, object? state) : ITimer
        {
            private bool _enabled;
            private DateTimeOffset _due;
            private TimeSpan _period;

            internal void Set(TimeSpan dueTime, TimeSpan period)
            {
                if (dueTime == Timeout.InfiniteTimeSpan)
                {
                    _enabled = false;
                    return;
                }

                _period = period;
                _due = owner.Timestamp + dueTime;
                _enabled = true;
            }

            public void Fire(DateTimeOffset now)
            {
                if (!_enabled || _due > now)
                    return;

                if (_period == Timeout.InfiniteTimeSpan)
                {
                    _enabled = false;
                }
                else
                {
                    // A recurring timer: a non-zero period schedules the next shot; guard the zero case.
                    _due = now + (_period == TimeSpan.Zero ? TimeSpan.FromTicks(1) : _period);
                }

                callback(state);
            }

            public bool Change(TimeSpan dueTime, TimeSpan period)
            {
                Set(dueTime, period);
                return true;
            }

            public bool Change(TimeSpan dueTime, TimeSpan period, TimeSpan timeout)
            {
                Set(dueTime, period);
                return true;
            }

            public void Dispose() => owner.Unregister(this);

            public ValueTask DisposeAsync()
            {
                Dispose();
                return ValueTask.CompletedTask;
            }
        }
    }

    /// <summary>
    ///     Values pass through unchanged (minus their watermark), and the latest held watermark is released as a
    ///     bare progress carrier only after the periodic handler runs.
    /// </summary>
    [Fact]
    public async Task ValuesPassThroughStripped_AndTickReleasesHeldWatermark()
    {
        await using var flow = new FlowSource();
        var time = new ManualTimeProvider();
        long calls = 0;
        var period = TimeSpan.FromSeconds(3600);

        flow
            .OnAsyncConsumatorSource(out var writer, new[] { new Carrier<int>(1, Watermark.From(1)), new Carrier<int>(2, Watermark.From(2)) })
            .PeriodicRunner(() => Interlocked.Increment(ref calls))
            .Every(period, time)
            .Consume(out var reader);

        var runTask = flow.ExecuteAsync(TestContext.Current.CancellationToken);

        // Drain the two value carriers: the runner aggregates their watermarks (latest = 2) before emitting.
        var first = await reader.ReadAsync(TestContext.Current.CancellationToken);
        var second = await reader.ReadAsync(TestContext.Current.CancellationToken);

        foreach (var item in new[] { first, second })
        {
            Assert.True(item.HasValue);
            Assert.Equal(Watermark.Nothing, item.Watermark);
        }

        Assert.Equal(new[] { 1, 2 }, new[] { first.Value, second.Value }.OrderBy(static x => x));

        // A tick runs the handler and releases the held watermark as a bare progress carrier (heartbeat).
        // Reading the heartbeat is the deterministic sync point: it can only arrive after the handler ran.
        time.Advance(period);
        var heartbeat = await reader.ReadAsync(TestContext.Current.CancellationToken);
        Assert.False(heartbeat.HasValue);
        Assert.Equal(Watermark.From(2), heartbeat.Watermark);
        Assert.True(Volatile.Read(ref calls) >= 1);

        writer.TryComplete();
        await runTask;
    }

    /// <summary>The handler runs once per interval; a bare progress tick with no held watermark still runs it.</summary>
    [Fact(Timeout = 1000)]
    public async Task HandlerRunsPerInterval()
    {
        await using var flow = new FlowSource();
        var time = new ManualTimeProvider();
        var period = TimeSpan.FromSeconds(1);
        var ticks = Channel.CreateUnbounded<long>();

        flow
            .OnAsyncConsumatorSource<Carrier<int>>(out var writer)
            .PeriodicRunner(() => ticks.Writer.TryWrite(1L))
            .Every(period, time)
            .Consume(out var reader);

        var runTask = flow.ExecuteAsync(TestContext.Current.CancellationToken);

        // Each interval tick fires exactly one handler invocation, observed via the tick channel: reading
        // from it after each Advance is the deterministic sync point (the handler runs on the loop thread).
        time.Advance(period);
        await ticks.Reader.ReadAsync(TestContext.Current.CancellationToken);

        time.Advance(period);
        await ticks.Reader.ReadAsync(TestContext.Current.CancellationToken);

        writer.TryComplete();
        await runTask;

        // The two interval ticks produced their handler calls (read above); source completion runs the
        // terminal one once more (documented finalization) — exactly one more tick remains in the channel.
        Assert.Equal(1, ticks.Reader.Count);
    }

    /// <summary>
    ///     Bare progress input is absorbed (only its watermark is aggregated), never forwarded as a phantom
    ///     element; the final held watermark is released when the source completes.
    /// </summary>
    [Fact]
    public async Task BareProgressAbsorbed_AndReleasedOnCompletion()
    {
        await using var flow = new FlowSource();
        var time = new ManualTimeProvider();
        var period = TimeSpan.FromSeconds(3600);

        flow
            .OnAsyncConsumatorSource(out var writer, new[] { new Carrier<int>(Watermark.From(5)) })
            .PeriodicRunner(() => { })
            .Every(period, time)
            .Consume(out var reader);

        var runTask = flow.ExecuteAsync(TestContext.Current.CancellationToken);

        writer.TryComplete();
        await runTask;

        var items = await reader.ReadAllAsync(TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.DoesNotContain(items, static item => item.HasValue);

        var progress = Assert.Single(items);
        Assert.Equal(Watermark.From(5), progress.Watermark);
    }

    /// <summary>
    ///     With no tick fired, the values still pass through, and the held watermark is released only by the
    ///     terminal fill when the source completes.
    /// </summary>
    [Fact]
    public async Task CompletionReleasesLastWatermark_WithoutIntermediateTick()
    {
        await using var flow = new FlowSource();
        var time = new ManualTimeProvider();
        var period = TimeSpan.FromSeconds(3600);

        flow
            .OnAsyncConsumatorSource(out var writer, new[] { new Carrier<int>(42, Watermark.From(10)) })
            .PeriodicRunner(() => { })
            .Every(period, time)
            .Consume(out var reader);

        var runTask = flow.ExecuteAsync(TestContext.Current.CancellationToken);

        var value = await reader.ReadAsync(TestContext.Current.CancellationToken);
        Assert.True(value.HasValue);
        Assert.Equal(42, value.Value);
        Assert.Equal(Watermark.Nothing, value.Watermark);

        writer.TryComplete();
        await runTask;

        var rest = await reader.ReadAllAsync(TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        var progress = Assert.Single(rest);
        Assert.False(progress.HasValue);
        Assert.Equal(Watermark.From(10), progress.Watermark);
    }
}