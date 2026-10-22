using Steelax.Pufflow.Abstractions;
using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Sdk.Test;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Merging;

/// <summary>
///     Black-box tests for the merge of several pull sources (backed by <c>MergeProcessor</c>): several
///     independent streams are built on a single flow, terminated with <c>Expose</c> to obtain their
///     <see cref="IAsyncConsumator{T}" /> side, and then combined with <c>MergeSources</c>. The merged
///     stream is observed through a sink reader.
/// </summary>
/// <remarks>
///     Sources emit <see cref="Carrier{T}" /> items with real watermarks — the barrier that merge is built on
///     collects round watermarks from the inputs, so progress-less (<see cref="Watermark.Nothing" />) inputs
///     cannot advance it. This mirrors the real usage (Kafka) where every emitted carrier carries a watermark.
/// </remarks>
public static partial class MergeProcessorTests
{
    private const int TimeoutMs = 2_000;

    public sealed class MergeSources
    {
        /// <summary>
        ///     Values from every input source are delivered by the merged stream: the full set is preserved and
        ///     the per-source order is not violated (merge interleaves sources, so global order is not fixed).
        /// </summary>
        [Fact(Timeout = TimeoutMs)]
        public async Task AllSources_AreMergedWithValuesPreserved()
        {
            await using var flow = new FlowSource();

            flow.OnAsyncConsumatorSource(SeqValues((1, 10), (2, 20), (3, 30))).Expose(out var first);
            flow.OnAsyncConsumatorSource(SeqValues((10, 10), (20, 20), (30, 30))).Expose(out var second);
            flow.MergeSources(first, second).Consume(out var reader);

            await flow.ExecuteAsync(TestContext.Current.CancellationToken);
            var results = await reader.ReadAllAsync(TestContext.Current.CancellationToken)
                .ToListAsync(TestContext.Current.CancellationToken);

            var values = results.Where(static r => r.HasValue).Select(static r => r.Value).ToList();

            Assert.Equal(6, values.Count);
            Assert.Equal([1, 2, 3], values.Where(static v => v < 10));
            Assert.Equal([10, 20, 30], values.Where(static v => v >= 10));

            // Progress never goes backwards: every emitted watermark is non-decreasing.
            var progress = results.Where(static r => r.HasWatermark).Select(static r => r.Watermark).ToList();
            Assert.OrderIncreasing(progress, false);

            // The stream closes with a bare carrier restoring the maximum watermark seen across the sources.
            Assert.False(results[^1].HasValue, "the last item must be a bare progress carrier");
            Assert.Equal(Watermark.From(30), results[^1].Watermark);
        }

        /// <summary>A single source streams through the merge unchanged, in order.</summary>
        [Fact(Timeout = TimeoutMs)]
        public async Task SingleSource_YieldsItsValuesInOrder()
        {
            await using var flow = new FlowSource();

            flow.OnAsyncConsumatorSource(SeqValues((1, 10), (2, 20), (3, 30), (4, 40))).Expose(out var source);
            flow.MergeSources(source).Consume(out var reader);

            await flow.ExecuteAsync(TestContext.Current.CancellationToken);
            var results = await reader.ReadAllAsync(TestContext.Current.CancellationToken)
                .ToListAsync(TestContext.Current.CancellationToken);

            Assert.Equal([1, 2, 3, 4], results.Where(static r => r.HasValue).Select(static r => r.Value));

            // Progress never goes backwards: every emitted watermark is non-decreasing.
            var progress = results.Where(static r => r.HasWatermark).Select(static r => r.Watermark).ToList();
            Assert.OrderIncreasing(progress, false);

            // The stream closes with a bare carrier restoring the maximum watermark seen across the sources.
            Assert.False(results[^1].HasValue, "the last item must be a bare progress carrier");
            Assert.Equal(Watermark.From(40), results[^1].Watermark);
        }

        /// <summary>An empty source contributes nothing; values from the other sources still flow.</summary>
        [Fact(Timeout = TimeoutMs)]
        public async Task EmptySource_DoesNotBlockTheMergedStream()
        {
            await using var flow = new FlowSource();

            flow.OnAsyncConsumatorSource(SeqValues((1, 10), (2, 20))).Expose(out var first);
            flow.OnAsyncConsumatorSource(SeqValues()).Expose(out var empty);
            flow.OnAsyncConsumatorSource(SeqValues((3, 10))).Expose(out var last);
            flow.MergeSources(first, empty, last).Consume(out var reader);

            await flow.ExecuteAsync(TestContext.Current.CancellationToken);
            var results = await reader.ReadAllAsync(TestContext.Current.CancellationToken)
                .ToListAsync(TestContext.Current.CancellationToken);

            var values = results.Where(static r => r.HasValue).Select(static r => r.Value).ToList();
            values.Sort();
            Assert.Equal([1, 2, 3], values);

            // Progress never goes backwards: every emitted watermark is non-decreasing.
            var progress = results.Where(static r => r.HasWatermark).Select(static r => r.Watermark).ToList();
            Assert.OrderIncreasing(progress, false);

            // The stream closes with a bare carrier restoring the maximum watermark seen across the sources.
            Assert.False(results[^1].HasValue, "the last item must be a bare progress carrier");
            Assert.Equal(Watermark.From(20), results[^1].Watermark);
        }

        /// <summary>
        ///     Merging a single empty stream completes cleanly: the reader terminates with no values (the
        ///     progress-less barrier produces no terminal bare carrier) and the execution finishes without
        ///     hanging.
        /// </summary>
        [Fact(Timeout = TimeoutMs)]
        public async Task OnlyEmptySource_CompletesWithNoValues()
        {
            await using var flow = new FlowSource();

            flow.OnAsyncConsumatorSource(new List<Carrier<int>>()).Expose(out var empty);
            flow.MergeSources(empty).Consume(out var reader);

            await flow.ExecuteAsync(TestContext.Current.CancellationToken);
            var results = await reader.ReadAllAsync(TestContext.Current.CancellationToken)
                .ToListAsync(TestContext.Current.CancellationToken);

            // No data was ever emitted; the initialized collection holds Nothing, so no bare carrier appears.
            Assert.Empty(results);
        }

        /// <summary>
        ///     The merged stream advances progress from every contributing input: bare (progress) carriers carry a
        ///     real, strictly advancing watermark produced by the merge barrier.
        /// </summary>
        [Fact(Timeout = TimeoutMs)]
        public async Task WatermarksFromSources_ArePropagatedByTheMerge()
        {
            await using var flow = new FlowSource();

            flow.OnAsyncConsumatorSource(SeqValues((1, 10), (2, 20))).Expose(out var first);
            flow.OnAsyncConsumatorSource(SeqValues((3, 10), (4, 20))).Expose(out var second);
            flow.MergeSources(first, second).Consume(out var reader);

            await flow.ExecuteAsync(TestContext.Current.CancellationToken);
            var results = await reader.ReadAllAsync(TestContext.Current.CancellationToken)
                .ToListAsync(TestContext.Current.CancellationToken);

            // Real progress is carried by bare (empty) carriers only; every emitted watermark is non-decreasing.
            var progress = results.Where(static r => r.HasWatermark).Select(static r => r.Watermark).ToList();
            Assert.NotEmpty(progress);
            Assert.All(progress, static w => Assert.False(w.IsNothing));
            Assert.OrderIncreasing(progress, false);

            // Data carriers are forwarded intact: every input value is present.
            Assert.Equal(4, results.Count(static r => r.HasValue));

            // The stream closes with a bare carrier restoring the maximum watermark seen across the sources.
            Assert.False(results[^1].HasValue, "the last item must be a bare progress carrier");
            Assert.Equal(Watermark.From(20), results[^1].Watermark);
        }
    }
}