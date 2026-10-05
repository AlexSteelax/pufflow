using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Sdk.Test;
using System.Threading.Channels;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Chunking;

public static partial class CarrierChunkProcessorTests
{
    /// <summary>
    ///     Watermark propagation through <c>Chunking</c>: each emitted <see cref="Carrier{ChunkOfT}" /> must carry the
    ///     maximum watermark of every item (data or bare progress) that arrived within its window, and the watermark
    ///     must never be lost, stick to the next window, or be invented for items that carried none.
    /// </summary>
    public sealed class WatermarkSequence
    {
        [Fact(Timeout = TimeoutMs)]
        public async Task EachWindow_CarriesItsOwnMaxWatermark()
        {
            var input = new Carrier<int>[]
            {
                new(1, Watermark.From(10)),
                new(2, Watermark.From(20)),
                new(3, Watermark.From(40)),
                new(4, Watermark.From(30)),
            };

            var windows = await RunAsync(input, size: 2, linger: TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            Assert.Equal(2, windows.Count);
            AssertDataWindow(windows[0], [1, 2], Watermark.From(20));
            AssertDataWindow(windows[1], [3, 4], Watermark.From(40));
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task Watermark_DoesNotLeakIntoNextWindow()
        {
            var input = new Carrier<int>[]
            {
                new(1, Watermark.From(100)),
                new(2, Watermark.From(100)),
                new(3, Watermark.From(5)),
                new(4, Watermark.From(5)),
            };

            var windows = await RunAsync(input, size: 2, linger: TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            // The second window legitimately carries a lower maximum: the first window's watermark must not stick.
            Assert.Equal(2, windows.Count);
            AssertDataWindow(windows[0], [1, 2], Watermark.From(100));
            AssertDataWindow(windows[1], [3, 4], Watermark.From(5));
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task BareProgress_RaisesCurrentWindowMax_WithoutConsumingSlot()
        {
            var input = new Carrier<int>[]
            {
                new(1, Watermark.From(10)),
                new Carrier<int>(Watermark.From(50)), // bare progress in the middle of the window
                new(2, Watermark.From(60)),
            };

            var windows = await RunAsync(input, size: 2, linger: TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            // The bare item does not occupy a buffer slot (both values fit one size-2 chunk) and its watermark
            // participates in the window maximum.
            var window = Assert.Single(windows);
            AssertDataWindow(window, [1, 2], Watermark.From(60));
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task LeadingBareProgress_ContributesToFirstWindow()
        {
            var input = new Carrier<int>[]
            {
                new Carrier<int>(Watermark.From(50)), // bare progress before any data
                new(1, Watermark.From(10)),
                new(2, Watermark.From(20)),
            };

            var windows = await RunAsync(input, size: 2, linger: TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            var window = Assert.Single(windows);
            AssertDataWindow(window, [1, 2], Watermark.From(50));
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task WatermarkFromBareItemsOnly_AttachesToDataChunk()
        {
            // Data values carry no watermark; the window maximum comes exclusively from bare progress items.
            var input = new Carrier<int>[]
            {
                new(1, Watermark.Nothing()),
                new Carrier<int>(Watermark.From(7)),
                new(2, Watermark.Nothing()),
            };

            var windows = await RunAsync(input, size: 2, linger: TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            var window = Assert.Single(windows);
            AssertDataWindow(window, [1, 2], Watermark.From(7));
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task TrailingBareProgress_FlushesAsEmptyCarrierAtEof()
        {
            var input = new Carrier<int>[]
            {
                new(1, Watermark.From(10)),
                new(2, Watermark.From(20)),
                new Carrier<int>(Watermark.From(50)),
                new Carrier<int>(Watermark.From(60)),
            };

            var windows = await RunAsync(input, size: 2, linger: TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            // [1,2] is flushed by the count trigger; the trailing progress items carry no data, so at EOF they are
            // emitted as a single empty carrier holding their maximum.
            Assert.Equal(2, windows.Count);
            AssertDataWindow(windows[0], [1, 2], Watermark.From(20));
            AssertBareWindow(windows[1], Watermark.From(60));
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task LingerFlush_DataChunk_CarriesWindowMax()
        {
            await using var flow = new FlowSource();
            var windows = await RunTimedAsync(
                flow,
                size: 10,
                TimeSpan.FromMilliseconds(30),
                writer =>
                {
                    // Both values arrive inside the linger window; the linger flush must preserve the window maximum.
                    writer.TryWrite(new Carrier<int>(1, Watermark.From(10)));
                    writer.TryWrite(new Carrier<int>(2, Watermark.From(30)));
                    return Task.Delay(150);
                },
                TestContext.Current.CancellationToken);

            var window = Assert.Single(windows);
            AssertDataWindow(window, [1, 2], Watermark.From(30));
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task BareOnly_EachProgressItem_FlushedWithOwnWatermark()
        {
            // A long idling stream of bare progress items: each one must flow through as its own empty carrier
            // carrying its own watermark (not be swallowed or merged into a later one).
            await using var flow = new FlowSource();
            var windows = await RunTimedAsync(
                flow,
                size: 100,
                TimeSpan.FromMilliseconds(30),
                async writer =>
                {
                    writer.TryWrite(new Carrier<int>(Watermark.From(10)));
                    await Task.Delay(120);
                    writer.TryWrite(new Carrier<int>(Watermark.From(20)));
                    await Task.Delay(120);
                    writer.TryWrite(new Carrier<int>(Watermark.From(30)));
                    await Task.Delay(120);
                },
                TestContext.Current.CancellationToken);

            Assert.Equal(3, windows.Count);
            AssertBareWindow(windows[0], Watermark.From(10));
            AssertBareWindow(windows[1], Watermark.From(20));
            AssertBareWindow(windows[2], Watermark.From(30));
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task MixedWindows_CountAndLinger_CarryOwnWatermarks()
        {
            await using var flow = new FlowSource();
            var windows = await RunTimedAsync(
                flow,
                size: 2,
                TimeSpan.FromMilliseconds(30),
                async writer =>
                {
                    // [1,2] flushed by count, then [3] flushed by linger.
                    writer.TryWrite(new Carrier<int>(1, Watermark.From(10)));
                    writer.TryWrite(new Carrier<int>(2, Watermark.From(20)));
                    await Task.Delay(120);
                    writer.TryWrite(new Carrier<int>(3, Watermark.From(40)));
                    await Task.Delay(120);
                },
                TestContext.Current.CancellationToken);

            Assert.Equal(2, windows.Count);
            AssertDataWindow(windows[0], [1, 2], Watermark.From(20));
            AssertDataWindow(windows[1], [3], Watermark.From(40));
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task PartialChunkAtEof_CarriesItsWatermark()
        {
            var input = new Carrier<int>[]
            {
                new(1, Watermark.From(5)),
            };

            var windows = await RunAsync(input, size: 10, linger: TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            var window = Assert.Single(windows);
            AssertDataWindow(window, [1], Watermark.From(5));
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task MonotonicInput_YieldsNonDecreasingWatermarks_ClosingOnMax()
        {
            // A realistic provider: non-decreasing watermarks, repeated within a tick. Slicing it into chunks must
            // never make progress go backwards and the final watermark must equal the input maximum.
            const int n = 100;

            var input = Enumerable.Range(0, n)
                .Select(i => new Carrier<int>(i, Watermark.From((i + 1) * 10 / 3)))
                .ToArray();

            var windows = await RunAsync(input, size: 7, linger: TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            // Every value is emitted exactly once, in order.
            Assert.Equal(Enumerable.Range(0, n), windows.SelectMany(static w => w.Items));

            // Progress never goes backwards.
            var real = windows.Select(static w => w.Watermark).Where(static w => !w.IsNothing).ToArray();
            Assert.NotEmpty(real);
            Assert.OrderIncreasing(real, false);

            // The maximum of the emitted watermarks is the maximum of the input.
            Assert.Equal(Watermark.From(n * 10 / 3), real.Max());
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task ZeroInputWatermark_IsFaithfullyPropagatedAsZero()
        {
            // If the upstream really attaches Watermark.From(0) to the items, the chunker must forward it —
            // it must not fabricate a larger value, nor turn it into Nothing. Seeing 0 downstream therefore
            // means the source carried 0, not that the chunker strips the watermark.
            var input = new Carrier<int>[]
            {
                new(1, Watermark.From(0)),
                new(2, Watermark.From(0)),
            };

            var windows = await RunAsync(input, size: 2, linger: TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            var window = Assert.Single(windows);
            AssertDataWindow(window, [1, 2], Watermark.From(0));
            Assert.False(window.Watermark.IsNothing);
        }
    }
}