using Steelax.Pufflow.Operators.Aggregators.Warming;
using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Sdk.Test;
using Unio;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Warming;

/// <summary>
///     Black-box tests for <see cref="WarmProcessor{TKey,TValue,TGroup,TWarm}" />: a source enumerator is
///     pushed through the operator and the resulting <see cref="Unio{T0,T1,T2}" /> stream is observed.
/// </summary>
public static partial class WarmProcessorTests
{
    private const int NoLingerMs = 60_000;

    private static async Task<List<Unio<int, TGroup, Watermark>>> RunAsync<TGroup>(
        IJobFactory<int, string> jobFactory,
        IWarmPolicy<int, string> policy,
        IWarmAccumulatorFactory<int, int, TGroup> accumulatorFactory,
        IReadOnlyList<Watermarked<int>> input,
        FlowSource flow,
        TimeSpan? watchdogPeriod = null)
    {
        var options = new WarmOptions
        {
            MaxConcurrency = 1,
            MaxQueued = 8,
            SegmentCapacity = 4,
            SegmentLinger = TimeSpan.FromMilliseconds(NoLingerMs),
            QueueWeightLimit = 1000,
            WatchdogPeriod = watchdogPeriod ?? Timeout.InfiniteTimeSpan
        };

        flow
            .OnAsyncConsumatorSource(input)
            .Warming(
                options,
                jobFactory,
                ValueToKey,
                policy,
                accumulatorFactory)
            .Consume(out var reader);

        await flow.ExecuteAsync();

        return await reader.ReadAllAsync(TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);
    }

    private static async Task WaitUntilAsync(Func<bool> condition, CancellationToken token)
    {
        while (!condition() && !token.IsCancellationRequested)
            await Task.Delay(10, token);
    }

    // Accumulator: collects int values and emits a single string group on consumption.
    private sealed class ListAccumulator : WarmAccumulator<int, string>
    {
        private readonly List<int> _values = new();
        private int _consumed;

        protected override void Add(int value)
        {
            _values.Add(value);
        }

        protected override bool TryConsume(out string group, out int weight)
        {
            if (_consumed >= _values.Count)
            {
                group = string.Empty;
                weight = 0;
                return false;
            }

            group = string.Join(",", _values.Skip(_consumed));
            weight = _values.Count - _consumed;
            _consumed = _values.Count;
            return true;
        }
    }

    private sealed class ListAccumulatorFactory : IWarmAccumulatorFactory<int, int, string>
    {
        public WarmAccumulator<int, string> Create(int key)
        {
            return new ListAccumulator();
        }
    }

    // Honest queue accumulator: TValue == TGroup, each value is stored in a queue and
    // released exactly one at a time — without collapsing into a string group.
    private sealed class QueueAccumulator : WarmAccumulator<int, int>
    {
        private readonly Queue<int> _values = new();

        protected override void Add(int value)
        {
            _values.Enqueue(value);
        }

        protected override bool TryConsume(out int group, out int weight)
        {
            if (_values.Count == 0)
            {
                group = 0;
                weight = 0;
                return false;
            }

            group = _values.Dequeue();
            weight = 1;
            return true;
        }
    }

    private sealed class QueueAccumulatorFactory : IWarmAccumulatorFactory<int, int, int>
    {
        public WarmAccumulator<int, int> Create(int key)
        {
            return new QueueAccumulator();
        }
    }

    // Policy: warms even keys by default; OnWarmed collects the results.
    private sealed class TestPolicy : IWarmPolicy<int, string>
    {
        public bool WarmEvenOnly { get; } = true;

        public List<(int Key, string Warm)> Warmed { get; } = new();

        public bool ShouldWarm(int key)
        {
            return WarmEvenOnly ? key % 2 == 0 : key % 2 != 0;
        }

        public void OnWarmed(int key, string warm)
        {
            Warmed.Add((key, warm));
        }
    }

    // Policy with an arbitrary predicate: warms keys matching the condition (for a non-uniform mixed mode).
    private sealed class PredicatePolicy(Func<int, bool> predicate) : IWarmPolicy<int, string>
    {
        public bool ShouldWarm(int key)
        {
            return predicate(key);
        }

        public void OnWarmed(int key, string warm)
        {
        }
    }

    private sealed class SyncJob : IAsyncJob<int, string>
    {
        private KeyValuePair<int, string>[] _results = [];

        public Task ExecuteAsync(int[] keys, CancellationToken cancellationToken)
        {
            _results = keys.Select(k => new KeyValuePair<int, string>(k, "W" + k)).ToArray();
            return Task.CompletedTask;
        }

        public ReadOnlySpan<KeyValuePair<int, string>> GetResult()
        {
            return _results.AsSpan();
        }

        public void SynchronousComplete()
        {
        }

        public void Dispose()
        {
        }
    }

    private sealed class SyncJobFactory : IJobFactory<int, string>
    {
        public IAsyncJob<int, string> CreateAsyncJob()
        {
            return new SyncJob();
        }
    }

    // Delayed job: emulates a "real" warm task that takes some time to complete.
    private sealed class DelayedJob(int delayMs) : IAsyncJob<int, string>
    {
        private KeyValuePair<int, string>[] _results = Array.Empty<KeyValuePair<int, string>>();

        public async Task ExecuteAsync(int[] keys, CancellationToken cancellationToken)
        {
            await Task.Delay(delayMs, cancellationToken);
            _results = keys.Select(k => new KeyValuePair<int, string>(k, "W" + k)).ToArray();
        }

        public ReadOnlySpan<KeyValuePair<int, string>> GetResult()
        {
            return _results.AsSpan();
        }

        public void SynchronousComplete()
        {
        }

        public void Dispose()
        {
        }
    }

    private sealed class DelayedJobFactory(int delayMs) : IJobFactory<int, string>
    {
        public IAsyncJob<int, string> CreateAsyncJob()
        {
            return new DelayedJob(delayMs);
        }
    }

    // Job with controlled completion (determinism for the cancellation test).
    private sealed class TcsJob : IAsyncJob<int, string>
    {
        public TaskCompletionSource Tcs { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int[]? Keys { get; private set; }

        public CancellationToken CancellationToken { get; private set; }

        public bool Started { get; private set; }

        public Task ExecuteAsync(int[] keys, CancellationToken cancellationToken)
        {
            Keys = keys;
            CancellationToken = cancellationToken;
            Started = true;
            return Tcs.Task;
        }

        public ReadOnlySpan<KeyValuePair<int, string>> GetResult()
        {
            return Keys!.Select(k => new KeyValuePair<int, string>(k, "W" + k)).ToArray().AsSpan();
        }

        public void SynchronousComplete()
        {
        }

        public void Dispose()
        {
        }
    }

    private sealed class TcsJobFactory(TcsJob job) : IJobFactory<int, string>
    {
        public IAsyncJob<int, string> CreateAsyncJob()
        {
            return job;
        }
    }
    
    private static int ValueToKey(scoped in int value) => value;
}
