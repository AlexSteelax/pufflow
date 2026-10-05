using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Chunking;

public static partial class CarrierChunkProcessorTests
{
    /// <summary>Data-only scenarios: no watermark handling is involved, the <see cref="Carrier{T}" /> wrapper must be transparent.</summary>
    public sealed class Data
    {
        [Fact(Timeout = TimeoutMs)]
        public async Task FillsBySize_FlushesTrailingOnEof()
        {
            var input = new Carrier<int>[]
            {
                new(1, Watermark.Nothing()),
                new(2, Watermark.Nothing()),
                new(3, Watermark.Nothing()),
                new(4, Watermark.Nothing()),
                new(5, Watermark.Nothing()),
            };

            var windows = await RunAsync(input, size: 2, linger: TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            Assert.Equal(3, windows.Count);
            AssertDataWindow(windows[0], [1, 2], Watermark.Nothing());
            AssertDataWindow(windows[1], [3, 4], Watermark.Nothing());
            AssertDataWindow(windows[2], [5], Watermark.Nothing());
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task ExactFill_FlushesImmediately()
        {
            var input = new Carrier<int>[]
            {
                new(1, Watermark.Nothing()),
                new(2, Watermark.Nothing()),
                new(3, Watermark.Nothing()),
            };

            var windows = await RunAsync(input, size: 3, linger: TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            var window = Assert.Single(windows);
            AssertDataWindow(window, [1, 2, 3], Watermark.Nothing());
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task EmptySource_YieldsNoWindows()
        {
            var windows = await RunAsync([], size: 3, linger: TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            Assert.Empty(windows);
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task NoWatermarkData_EmitsNothingWatermark()
        {
            // Items without a watermark (Watermark.Nothing) must not fabricate one on the way out: the emitted
            // carrier reports HasWatermark == false and its watermark is the Nothing sentinel.
            var input = new Carrier<int>[]
            {
                new(1, Watermark.Nothing()),
                new(2, Watermark.Nothing()),
            };

            var windows = await RunAsync(input, size: 2, linger: TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            var window = Assert.Single(windows);
            AssertDataWindow(window, [1, 2], Watermark.Nothing());
            Assert.True(window.Watermark.IsNothing);
        }
    }
}