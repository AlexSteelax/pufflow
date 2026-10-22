using System.Threading.Channels;
using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Sdk.Test;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Merging;

/// <summary>
///     Regression tests for the ready-bit re-arming inside <c>MergeProcessor</c>. A source consumed through
///     <c>TryPendingGet</c> after it signalled readiness consumes its ready bit; without an explicit re-arm the
///     input becomes invisible to the round-robin sweep when its index sits ahead of the current
///     <c>_globalIdx</c> (or when the sweep has already passed it), which can strand a live source and hang the
///     very next pass. These tests feed sources lazily through a push-side writer so readiness arrives
///     asynchronously — exactly the path that exercises <c>TryPendingGet</c>.
/// </summary>
public static partial class MergeProcessorTests
{
    public sealed class LazyReadySources
    {
        private const int TimeoutMs = 3_000;

        /// <summary>Sends carriers to the source on demand, with a yield between batches.</summary>
        private static async Task PushAsync(
            ChannelWriter<Carrier<int>> writer,
            IEnumerable<int> values,
            int stepMs,
            CancellationToken cancellationToken)
        {
            foreach (var value in values)
            {
                await Task.Delay(stepMs, cancellationToken);
                Assert.True(writer.TryWrite(new Carrier<int>(value, Watermark.Nothing)));
            }
        }

        /// <summary>
        ///     Two lazily-fed sources where reads alternate: readiness for one source can arrive while the other
        ///     is mid-sweep, so the ready bit of the prefetched source is consumed and must be re-armed, otherwise
        ///     the merge hangs waiting for values that have already been pushed.
        /// </summary>
        [Theory]
        [InlineData(1, 2)]
        [InlineData(2, 1)]
        [InlineData(1, 3)]
        [InlineData(3, 1)]
        public async Task AlternatingPush_DoesNotStrandReadySources(int firstBatch, int secondBatch)
        {
            await using var flow = new FlowSource();

            flow.OnAsyncConsumatorSource<Carrier<int>>(out var writerA, null).Expose(out var sourceA);
            flow.OnAsyncConsumatorSource<Carrier<int>>(out var writerB, null).Expose(out var sourceB);
            flow.MergeSources(sourceA, sourceB).Consume(out var reader);

            // Prefill one batch to kick off the loop, then feed the rest lazily on both sides.
            Assert.True(writerA.TryWrite(new Carrier<int>(firstBatch, Watermark.Nothing)));
            Assert.True(writerB.TryWrite(new Carrier<int>(secondBatch, Watermark.Nothing)));

            var pushA = PushAsync(writerA, [firstBatch + 10, firstBatch + 20], 20, TestContext.Current.CancellationToken);
            var pushB = PushAsync(writerB, [secondBatch + 10, secondBatch + 20], 20, TestContext.Current.CancellationToken);

            var exec = flow.ExecuteAsync(TestContext.Current.CancellationToken);

            await Task.WhenAll(pushA, pushB);
            writerA.TryComplete();
            writerB.TryComplete();

            await exec.WaitAsync(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);

            var values = (await reader.ReadAllAsync(TestContext.Current.CancellationToken)
                    .ToListAsync(TestContext.Current.CancellationToken))
                .Where(static r => r.HasValue)
                .Select(static r => r.Value)
                .ToList();

            values.Sort();
            var expected = new List<int>([firstBatch, firstBatch + 10, firstBatch + 20, secondBatch, secondBatch + 10, secondBatch + 20]);
            expected.Sort();
            Assert.Equal(expected, values);
        }

        /// <summary>
        ///     A single lazily-fed source: after the initial values are drained, repeatable reads (round-robin)
        ///     keep finding new batches pushed on demand. Without re-arming readiness this would stall after
        ///     the first drained batch.
        /// </summary>
        [Fact(Timeout = TimeoutMs)]
        public async Task SingleLazySource_RepeatedBatches_AreAllDelivered()
        {
            await using var flow = new FlowSource();

            flow.OnAsyncConsumatorSource<Carrier<int>>(out var writer, null).Expose(out var source);
            flow.MergeSources(source).Consume(out var reader);

            const int total = 5;

            var feeds = Enumerable.Range(1, total - 1)
                .Select(i => Task.Run(async () =>
                {
                    await Task.Delay(i * 30, TestContext.Current.CancellationToken);
                    Assert.True(writer.TryWrite(new Carrier<int>(i, Watermark.Nothing)));
                }))
                .ToList();

            Assert.True(writer.TryWrite(new Carrier<int>(0, Watermark.Nothing)));

            var exec = flow.ExecuteAsync(TestContext.Current.CancellationToken);

            await Task.WhenAll(feeds);
            writer.TryComplete();

            await exec.WaitAsync(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);

            var values = (await reader.ReadAllAsync(TestContext.Current.CancellationToken)
                    .ToListAsync(TestContext.Current.CancellationToken))
                .Where(static r => r.HasValue)
                .Select(static r => r.Value)
                .ToList();

            values.Sort();
            Assert.Equal(Enumerable.Range(0, total), values);
        }
    }
}