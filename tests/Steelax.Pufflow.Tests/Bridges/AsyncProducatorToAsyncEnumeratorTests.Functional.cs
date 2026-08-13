using Steelax.Pufflow.Bridges;

namespace Steelax.Pufflow.Tests.Bridges;

public static partial class AsyncProducatorToAsyncEnumeratorTests
{
    public sealed class Functional
    {
        [Fact]
        public async Task ReadInOrder_YieldsWrittenItems()
        {
            var bridge = new AsyncProducatorToAsyncEnumerator<int>(8);

            for (var i = 0; i < 5; i++)
                Assert.True(bridge.TryWrite(i));

            var collected = new List<int>();
            while (collected.Count < 5 && await bridge.MoveNextAsync())
                collected.Add(bridge.Current);

            Assert.Equal(new[] { 0, 1, 2, 3, 4 }, collected);
        }

        [Fact]
        public void FullBuffer_Overflows()
        {
            var bridge = new AsyncProducatorToAsyncEnumerator<int>(2);

            Assert.True(bridge.TryWrite(1));
            Assert.True(bridge.TryWrite(2));
            Assert.False(bridge.TryWrite(3));
        }

        [Fact]
        public async Task ConsumerWaitsUntilData_ThenReads()
        {
            var bridge = new AsyncProducatorToAsyncEnumerator<int>(4);

            var moveNext = bridge.MoveNextAsync();
            await Task.Delay(50, TestContext.Current.CancellationToken);

            Assert.False(moveNext.IsCompleted); // no data yet — still pending

            Assert.True(bridge.TryWrite(42));

            Assert.True(await moveNext);
            Assert.Equal(42, bridge.Current);
        }

        [Fact]
        public async Task ProducerWaitsUntilSpace_ThenWrites()
        {
            var bridge = new AsyncProducatorToAsyncEnumerator<int>(1);
            Assert.True(bridge.TryWrite(1));

            var wait = bridge.WaitToWriteAsync();
            await Task.Delay(50, TestContext.Current.CancellationToken);

            Assert.False(wait.IsCompleted); // no space yet — still pending

            Assert.True(await bridge.MoveNextAsync());
            Assert.Equal(1, bridge.Current);

            await wait; // producer resumes once a slot is freed
            Assert.True(bridge.TryWrite(2));
        }

        [Fact]
        public async Task Complete_EndOfStream()
        {
            var bridge = new AsyncProducatorToAsyncEnumerator<int>(4);
            bridge.Complete();

            Assert.False(await bridge.MoveNextAsync());
        }

        [Fact]
        public async Task CompleteAfterData_DrainsThenEnds()
        {
            var bridge = new AsyncProducatorToAsyncEnumerator<int>(4);
            Assert.True(bridge.TryWrite(1));
            Assert.True(bridge.TryWrite(2));
            bridge.Complete();

            var collected = new List<int>();
            while (await bridge.MoveNextAsync())
                collected.Add(bridge.Current);

            Assert.Equal(new[] { 1, 2 }, collected);
        }

        [Fact]
        public async Task Fault_ThrowsOnMoveNext()
        {
            var bridge = new AsyncProducatorToAsyncEnumerator<int>(4);
            var ex = new InvalidOperationException("boom");
            bridge.Complete(ex);

            var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () => await bridge.MoveNextAsync());
            Assert.Same(ex, thrown);
        }

        [Fact]
        public async Task WaitToWriteAfterComplete_Completes()
        {
            var bridge = new AsyncProducatorToAsyncEnumerator<int>(1);
            Assert.True(bridge.TryWrite(1)); // full

            var wait = bridge.WaitToWriteAsync();
            bridge.Complete();

            await wait; // must not hang
        }
    }
}