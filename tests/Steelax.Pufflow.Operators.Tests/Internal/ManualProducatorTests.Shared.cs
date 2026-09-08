using Steelax.Pufflow.Operators.Internal;
using Steelax.Toolkit.HighPerformance.Concurrency.Collections;

namespace Steelax.Pufflow.Operators.Tests.Internal;

/// <summary>
///     Shared path: a wrapper (not a <see cref="Conduit{T}" />) is dispatched by
///     <see cref="ManualProducator{T}.Create" /> to the <c>SharedManualProducator</c>, driven through
///     <c>WaitToWriteAsync</c> / <c>EventTask</c>. Because the wrapper merely decorates a conduit rather than
///     inheriting from it, <c>Create</c> falls back to the non-optimized interface-based implementation.
/// </summary>
public static partial class ManualProducatorTests
{
    public sealed class WithWrapper
    {
        private static ManualProducator<int> Create(Conduit<int> conduit) =>
            ManualProducator<int>.Create(new ConduitWrapper<int>(conduit));

        [Fact]
        public void TryWrite_AcceptsItemsInOrder()
        {
            var conduit = NewConduit();
            var producator = Create(conduit);

            Assert.True(producator.TryWrite(1));
            Assert.True(producator.TryWrite(2));

            Assert.True(conduit.TryRead(out var first));
            Assert.True(conduit.TryRead(out var second));

            Assert.Equal([1, 2], [first, second]);
        }

        [Fact]
        public void TryWrite_WhenFull_ReturnsFalse()
        {
            var conduit = NewConduit();
            var producator = Create(conduit);

            Assert.True(producator.TryWrite(1));
            Assert.True(producator.TryWrite(2));
            Assert.True(producator.TryWrite(3));
            Assert.True(producator.TryWrite(4));

            Assert.True(producator.IsFull);
            Assert.False(producator.TryWrite(5));
        }

        [Fact]
        public void TryComplete_ReflectsWrapperState()
        {
            var conduit = NewConduit();
            var producator = Create(conduit);

            Assert.False(conduit.IsCompleted);

            Assert.True(producator.TryComplete());

            Assert.True(conduit.IsCompleted);
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task OnReady_Fires_WhenSpaceBecomesAvailable()
        {
            var conduit = NewConduit();
            var producator = Create(conduit);
            var ready = new TaskCompletionSource();

            producator.OnReady += () => ready.TrySetResult();

            // Fill the conduit so the next write is rejected.
            Assert.True(producator.TryWrite(1));
            Assert.True(producator.TryWrite(2));
            Assert.True(producator.TryWrite(3));
            Assert.True(producator.TryWrite(4));
            Assert.False(producator.TryWrite(5));

            // Wake the producer so it registers the pending wait on the EventTask.
            Assert.False(producator.TryWrite(6));

            // Drain one slot to free capacity.
            Assert.True(conduit.TryRead(out _));

            await ready.Task.WaitAsync(TimeSpan.FromMilliseconds(TimeoutMs), TestContext.Current.CancellationToken);
            Assert.True(producator.TryWrite(7));
        }
    }
}