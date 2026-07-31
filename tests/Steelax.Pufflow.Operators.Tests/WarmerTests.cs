namespace Steelax.Pufflow.Operators.Tests;

/// <summary>
/// Unit tests for the <see cref="Warmer{TKey,TWarm}"/> class.
/// </summary>
public static partial class WarmerTests
{
    private const int NoLingerMs = 60_000;

    // ------------------------------------------------------------------
    //  Test doubles
    // ------------------------------------------------------------------

    /// <summary>Таймер, который можно поджигать вручную (для контроля linger).</summary>
    private sealed class ManualTimer : ITimer
    {
        private TimerCallback? _callback;
        private object? _state;

        public List<TimeSpan> ChangeCalls { get; } = [];

        public bool Change(TimeSpan dueTime, TimeSpan period)
        {
            ChangeCalls.Add(dueTime);
            return true;
        }

        public void Dispose() { }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        public void Configure(TimerCallback callback, object? state)
        {
            _callback = callback;
            _state = state;
        }

        public void Fire() => _callback?.Invoke(_state);
    }

    /// <summary>TimeProvider без реальных таймеров.</summary>
    private sealed class ManualTimeProvider : TimeProvider
    {
        public ManualTimer Timer { get; } = new();

        public override DateTimeOffset GetUtcNow() => DateTimeOffset.UnixEpoch;

        public override long GetTimestamp() => 0;

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            Timer.Configure(callback, state);
            return Timer;
        }
    }

    /// <summary>Синк, собирающий результат прогрева в порядке выдачи.</summary>
    private sealed class WarmSink : IWarmPolicy<int, string>
    {
        public List<(int Key, string Warm)> Items { get; } = [];

        public bool ShouldWarm(int key) => true;

        public void OnWarmed(int key, string warm) => Items.Add((key, warm));
    }

    /// <summary>Джоб, завершающийся синхронно (warm = "W" + key).</summary>
    private sealed class SyncJob : IAsyncJob<int, string>
    {
        private KeyValuePair<int, string>[] _results = [];

        public Task ExecuteAsync(int[] keys, CancellationToken cancellationToken)
        {
            _results = keys.Select(k => new KeyValuePair<int, string>(k, "W" + k)).ToArray();
            return Task.CompletedTask;
        }

        public ReadOnlySpan<KeyValuePair<int, string>> GetResult() => _results.AsSpan();

        public void SynchronousComplete() { }

        public void Dispose() { }
    }

    private sealed class SyncJobFactory : IJobFactory<int, string>
    {
        public int CreatedCount { get; private set; }

        public IAsyncJob<int, string> CreateAsyncJob()
        {
            CreatedCount++;
            return new SyncJob();
        }
    }

    /// <summary>Джоб, завершающийся с задержкой (warm = "W" + key).</summary>
    private sealed class DelayedJob(int delayMs) : IAsyncJob<int, string>
    {
        private KeyValuePair<int, string>[] _results = [];

        public async Task ExecuteAsync(int[] keys, CancellationToken cancellationToken)
        {
            _results = keys.Select(k => new KeyValuePair<int, string>(k, "W" + k)).ToArray();
            await Task.Delay(delayMs, cancellationToken);
        }

        public ReadOnlySpan<KeyValuePair<int, string>> GetResult() => _results.AsSpan();

        public void SynchronousComplete() { }

        public void Dispose() { }
    }

    private sealed class DelayedJobFactory(int delayMs) : IJobFactory<int, string>
    {
        public IAsyncJob<int, string> CreateAsyncJob() => new DelayedJob(delayMs);
    }

    /// <summary>Джоб, завершение которого контролируется тестом через TCS (детерминизм).</summary>
    private sealed class TcsJob : IAsyncJob<int, string>
    {
        private readonly TaskCompletionSource _tcs = new();
        private int[] _keys = [];

        public TaskCompletionSource Tcs => _tcs;
        public CancellationToken CancellationToken { get; private set; }
        public bool Disposed { get; private set; }

        public Task ExecuteAsync(int[] keys, CancellationToken cancellationToken)
        {
            _keys = keys;
            CancellationToken = cancellationToken;
            return _tcs.Task;
        }

        public ReadOnlySpan<KeyValuePair<int, string>> GetResult() =>
            _keys.Select(k => new KeyValuePair<int, string>(k, "W" + k)).ToArray().AsSpan();

        public void SynchronousComplete() { }

        public void Dispose() => Disposed = true;
    }

    private sealed class TcsJobFactory : IJobFactory<int, string>
    {
        public List<TcsJob> Created { get; } = [];

        public IAsyncJob<int, string> CreateAsyncJob()
        {
            var job = new TcsJob();
            Created.Add(job);
            return job;
        }
    }

    /// <summary>Джоб, всегда падающий с исключением.</summary>
    private sealed class FaultingJob : IAsyncJob<int, string>
    {
        public static readonly InvalidOperationException Boom = new("boom");

        public Task ExecuteAsync(int[] keys, CancellationToken cancellationToken) =>
            Task.FromException(Boom);

        public ReadOnlySpan<KeyValuePair<int, string>> GetResult() =>
            ReadOnlySpan<KeyValuePair<int, string>>.Empty;

        public void SynchronousComplete() { }

        public void Dispose() { }
    }

    private sealed class FaultingJobFactory : IJobFactory<int, string>
    {
        public IAsyncJob<int, string> CreateAsyncJob() => new FaultingJob();
    }

    /// <summary>Джоб, всегда отменяемый.</summary>
    private sealed class CanceledJob : IAsyncJob<int, string>
    {
        public Task ExecuteAsync(int[] keys, CancellationToken cancellationToken) =>
            Task.FromCanceled(new CancellationToken(true));

        public ReadOnlySpan<KeyValuePair<int, string>> GetResult() =>
            ReadOnlySpan<KeyValuePair<int, string>>.Empty;

        public void SynchronousComplete() { }

        public void Dispose() { }
    }

    private sealed class CanceledJobFactory : IJobFactory<int, string>
    {
        public IAsyncJob<int, string> CreateAsyncJob() => new CanceledJob();
    }

    private sealed class RecordingCallback
    {
        public int Count { get; private set; }

        public void Invoke() => Count++;
    }

    // ------------------------------------------------------------------
    //  Helpers
    // ------------------------------------------------------------------

    private static Warmer<int, string> Create(
        IJobFactory<int, string>? jobFactory = null,
        Action? onReady = null,
        ManualTimeProvider? timeProvider = null,
        int maxConcurrency = 2,
        int maxQueued = 4,
        int segmentCapacity = 2,
        TimeSpan? segmentLinger = null)
    {
        var warmer = new Warmer<int, string>(
            maxConcurrency,
            maxQueued,
            segmentCapacity,
            segmentLinger ?? TimeSpan.FromMilliseconds(NoLingerMs),
            jobFactory ?? new SyncJobFactory(),
            timeProvider ?? new ManualTimeProvider());

        if (onReady is not null)
            warmer.OnReady += onReady;

        return warmer;
    }

    private static void AddKeys(Warmer<int, string> warmer, params (int Key, long Watermark)[] keys)
    {
        foreach (var (key, watermark) in keys)
            warmer.AddKey(key, Watermark.From(watermark));
    }
}
