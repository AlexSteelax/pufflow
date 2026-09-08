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

    /// <summary>Default tuning for the black-box tests: a very large TTL so segments are only sealed by size.</summary>
    private static WarmOptions DefaultOptions(TimeSpan? watchdogPeriod = null) => new WarmOptions
    {
        MaxConcurrency = 1,
        MaxQueued = 8,
        SegmentCapacity = 4,
        SegmentTtl = TimeSpan.FromMilliseconds(NoLingerMs),
        QueueWeightLimit = 1000,
        WatchdogPeriod = watchdogPeriod ?? Timeout.InfiniteTimeSpan
    };

    private static async Task<List<Carrier<Unio<int, TGroup>>>> RunAsync<TGroup>(
        IJobFactory<int, string> jobFactory,
        IWarmPolicy<int, string> policy,
        IWarmAccumulatorFactory<int, int, TGroup> accumulatorFactory,
        IReadOnlyList<Carrier<int>> input,
        FlowSource flow,
        WarmOptions options,
        CancellationToken cancellationToken)
    {
        flow
            .OnAsyncConsumatorSource(input)
            .Warming(
                options,
                jobFactory,
                ValueToKey,
                policy,
                accumulatorFactory)
            .Consume(out var reader);

        await flow.ExecuteAsync(cancellationToken);

        return await reader.ReadAllAsync(TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);
    }

    // -- Value/group/progress extraction helpers over the Carrier<Unio<...>> output ------------------

    /// <summary>Pass-through values (carrier payload union T0) present in the output.</summary>
    private static TValue[] Values<TValue, TGroup>(IReadOnlyList<Carrier<Unio<TValue, TGroup>>> results)
    {
        return results.Where(static r => r is { HasValue: true, Value.IsT0: true }).Select(static r => r.Value.AsT0).ToArray();
    }

    /// <summary>Accumulated group results (carrier payload union T1) present in the output.</summary>
    private static TGroup[] Groups<TGroup>(IReadOnlyList<Carrier<Unio<int, TGroup>>> results)
    {
        return results.Where(static r => r is { HasValue: true, Value.IsT1: true }).Select(static r => r.Value.AsT1).ToArray();
    }

    /// <summary>The real progress watermarks carried by empty (bare) carriers.</summary>
    private static Watermark[] Progress<TValue, TGroup>(IReadOnlyList<Carrier<Unio<TValue, TGroup>>> results)
    {
        return results.Where(static w => w.HasWatermark).Select(static c => c.Watermark).ToArray();
    }

    /// <summary>Indicates whether the last output item is an empty (bare-progress) carrier.</summary>
    private static bool IsLastProgress<TValue, TGroup>(IReadOnlyList<Carrier<Unio<TValue, TGroup>>> results)
    {
        return results.Count > 0 && !results[^1].HasValue;
    }

    // Accumulator: collects int values and emits a single string group on consumption.
    private sealed class ListAccumulator : WarmAccumulator<int, string>
    {
        private readonly List<int> _values = [];
        private int _consumed;

        protected internal override int EstimatedWeight => 1;

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
    // released exactly one at a time вЂ” without collapsing into a string group.
    private sealed class QueueAccumulator : WarmAccumulator<int, int>
    {
        private readonly Queue<int> _values = new();

        protected internal override int EstimatedWeight => 1;

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

    private sealed class QueueAccumulatorFactory : IWarmAccumulatorFactory<int, int>
    {
        public WarmAccumulator<int, int> Create(int key)
        {
            return new QueueAccumulator();
        }
    }
    
    private static int ValueToKey(scoped in int value) => value;
    
    private static Func<int, bool> WarmEvenOnly => key => key % 2 == 0;
}