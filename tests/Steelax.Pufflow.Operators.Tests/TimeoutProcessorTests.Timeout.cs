namespace Steelax.Pufflow.Operators.Tests;

public static partial class TimeoutProcessorTests
{
    public sealed class Timeout
    {
        [Fact]
        public async Task IdleSource_EmitsAwaitTimeoutMarkers_ThenResumes()
        {
            var processor = new TimeoutProcessor<int>(TimeSpan.FromMilliseconds(30));
            var result = await CollectAsync(processor, SegmentedSourceAsync((new[] { 1 }, 120), (new[] { 2 }, 0)));

            // The first element arrives in time; the 120ms idle gap (> 30ms timeout) produces at
            // least one marker; the per-wait window then re-arms and the source resumes.
            Assert.True(result[0].IsT0);
            Assert.Equal(1, result[0].AsT0);

            Assert.Contains(result, value => value.IsT1);

            Assert.True(result[^1].IsT0);
            Assert.Equal(2, result[^1].AsT0);
        }

        [Fact]
        public async Task SyncBurst_ThenAsync_DoesNotFault()
        {
            // Two consecutive synchronous items (exercising the fast path), then an async gap.
            // The fast path must not leave a stale SourceSlot signal in the fan-in, otherwise the
            // pending transition is misread as a terminal state.
            var processor = new TimeoutProcessor<int>(TimeSpan.FromSeconds(5));
            var result = await CollectAsync(processor, SyncBurstThenAsyncSourceAsync());

            Assert.All(result, value => Assert.True(value.IsT0));
            Assert.Equal(new[] { 1, 2, 3 }, result.Select(value => value.AsT0));
        }

        [Fact]
        public async Task ConsumerProcessingTime_DoesNotTripTimeout()
        {
            // The consumer spends longer than the timeout processing a value. Because the timer is
            // armed only while waiting for the source (and a stale timer signal is cleared on the
            // next item), no marker is emitted.
            var processor = new TimeoutProcessor<int>(TimeSpan.FromMilliseconds(30));
            await using var sourceEnumerator = new[] { 1, 2 }.ToAsyncEnumerable().GetAsyncEnumerator(TestContext.Current.CancellationToken);
            await using var enumerator = processor.GetAsyncEnumerator(sourceEnumerator, default);

            Assert.True(await enumerator.MoveNextAsync());
            Assert.True(enumerator.Current.IsT0);
            Assert.Equal(1, enumerator.Current.AsT0);

            await Task.Delay(120, TestContext.Current.CancellationToken); // consumer processing time

            Assert.True(await enumerator.MoveNextAsync());
            Assert.True(enumerator.Current.IsT0);
            Assert.Equal(2, enumerator.Current.AsT0);

            Assert.False(await enumerator.MoveNextAsync());
        }
    }
}