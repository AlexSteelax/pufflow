using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Sdk.Test;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Merging;

/// <summary>
///     Regression tests for <c>MergeSources</c> fed with carriers that carry no watermark
///     (<see cref="Watermark.Nothing" />). The merge barrier (<c>WatermarkBarrier</c>) only advances when every
///     source has reported a real watermark in the current round, so watermark-less inputs have no progress to
///     promote and the merged stream completes without a terminal (bare) carrier.
/// </summary>
/// <remarks>
///     Each test wires one or several <see cref="Watermark.Nothing" /> sources into
///     <c>MergeSources</c> and expects the stream to complete: values flow through, and the pipeline finishes
///     with no bare progress marker. These tests deliberately fail (time out) until the hang of the mixed
///     empty-source scenario is resolved — they document the expected observable behaviour.
/// </remarks>
public static partial class MergeProcessorTests
{
    public sealed class WatermarkLessSources
    {
        private const int TimeoutMs = 1_000;

        /// <summary>
        ///     A single source whose carriers carry no watermark must still stream its values through the merge
        ///     and reach end-of-stream (no final round watermark is expected — progress-less input has no progress
        ///     to report, but the stream must not hang).
        /// </summary>
        [Fact(Timeout = TimeoutMs)]
        public async Task SingleSource_WithNothingWatermarks_Completes()
        {
            await using var flow = new FlowSource();

            flow.OnAsyncConsumatorSource(SeqValuesNoWatermark([1, 2, 3])).Expose(out var source);
            flow.MergeSources(source).Consume(out var reader);

            await flow.ExecuteAsync(TestContext.Current.CancellationToken);
            var results = await reader.ReadAllAsync(TestContext.Current.CancellationToken)
                .ToListAsync(TestContext.Current.CancellationToken);

            Assert.Equal([1, 2, 3], results.Where(static r => r.HasValue).Select(static r => r.Value));

            // No watermark-less progress exists to promote: the barrier holds Nothing, so no bare carrier
            // is emitted and the last transmitted watermark is still Nothing.
            Assert.True(results[^1].Watermark.IsNothing, "the final watermark must be Nothing");
        }

        /// <summary>
        ///     Values from all watermark-less sources must be merged and the stream must complete — no input
        ///     carries progress, yet the merge should not stall waiting for a round that never fills.
        /// </summary>
        [Fact(Timeout = TimeoutMs)]
        public async Task MultipleSources_WithNothingWatermarks_Complete()
        {
            await using var flow = new FlowSource();

            flow.OnAsyncConsumatorSource(SeqValuesNoWatermark([1, 2])).Expose(out var first);
            flow.OnAsyncConsumatorSource(SeqValuesNoWatermark([3, 4])).Expose(out var second);
            flow.MergeSources(first, second).Consume(out var reader);

            await flow.ExecuteAsync(TestContext.Current.CancellationToken);
            var results = await reader.ReadAllAsync(TestContext.Current.CancellationToken)
                .ToListAsync(TestContext.Current.CancellationToken);

            var values = results.Where(static r => r.HasValue).Select(static r => r.Value).ToList();
            values.Sort();
            Assert.Equal([1, 2, 3, 4], values);

            // No watermark-less progress exists to promote: the barrier holds Nothing, so no bare carrier
            // is emitted and the last transmitted watermark is still Nothing.
            Assert.True(results[^1].Watermark.IsNothing, "the final watermark must be Nothing");
        }

        /// <summary>
        ///     A mix of a watermark-less source and a watered source must not hang: the watered input advances
        ///     the barrier, the watermark-less one contributes only values, and the stream completes.
        /// </summary>
        [Fact(Timeout = TimeoutMs)]
        public async Task MixedSource_WithAndWithoutWatermarks_Completes()
        {
            await using var flow = new FlowSource();

            flow.OnAsyncConsumatorSource(SeqValuesNoWatermark([1, 2])).Expose(out var plain);
            flow.OnAsyncConsumatorSource(SeqValues((3, 10), (4, 20))).Expose(out var watered);
            flow.MergeSources(plain, watered).Consume(out var reader);

            await flow.ExecuteAsync(TestContext.Current.CancellationToken);
            var results = await reader.ReadAllAsync(TestContext.Current.CancellationToken)
                .ToListAsync(TestContext.Current.CancellationToken);

            var values = results.Where(static r => r.HasValue).Select(static r => r.Value).ToList();
            values.Sort();
            Assert.Equal([1, 2, 3, 4], values);

            // The watered source's progress is preserved even though a sibling carries none.
            var progress = results.Where(static r => r.HasWatermark).Select(static r => r.Watermark).ToList();
            Assert.NotEmpty(progress);

            // The stream closes with a bare carrier restoring the maximum watermark seen across the sources.
            Assert.False(results[^1].HasValue, "the last item must be a bare progress carrier");
            Assert.Equal(Watermark.From(20), results[^1].Watermark);
        }

        /// <summary>
        ///     An empty source among watermark-less live sources must not stall the merge: previously the bare
        ///     source never contributed a round watermark, and with no other progress the merged stream never
        ///     produced its terminal round — the pipeline hung.
        /// </summary>
        [Fact(Timeout = TimeoutMs)]
        public async Task EmptySource_AmongWatermarkLessLiveSources_Completes()
        {
            await using var flow = new FlowSource();
            
            flow.OnAsyncConsumatorSource(SeqValuesNoWatermark([1])).Expose(out var first);
            flow.OnAsyncConsumatorSource(new List<Carrier<int>>()).Expose(out var empty);
            
            flow.MergeSources(first, empty).Consume(out var reader);

            await flow.ExecuteAsync(TestContext.Current.CancellationToken);
            var results = await reader.ReadAllAsync(TestContext.Current.CancellationToken)
                .ToListAsync(TestContext.Current.CancellationToken);

            var values = results.Where(static r => r.HasValue).Select(static r => r.Value).ToList();
            values.Sort();
            Assert.Equal([1], values);

            // No watermark-less progress exists to promote: the barrier holds Nothing, so no bare carrier
            // is emitted and the last transmitted watermark is still Nothing.
            Assert.True(results[^1].Watermark.IsNothing, "the final watermark must be Nothing");
        }
    }
}