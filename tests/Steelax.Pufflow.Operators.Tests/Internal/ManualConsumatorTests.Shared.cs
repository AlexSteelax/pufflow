using Steelax.Pufflow.Operators.Internal;
using Steelax.Toolkit.HighPerformance.Concurrency.Collections;

namespace Steelax.Pufflow.Operators.Tests.Internal;

/// <summary>
///     Shared path: a wrapper (not a <see cref="Conduit{T}" />) is dispatched by
///     <see cref="ManualConsumator{T}.Create" /> to the <c>SharedManualConsumator</c>, driven through
///     <c>WaitToReadAsync</c> / <c>EventTask</c>. Because the wrapper merely decorates a conduit rather than
///     inheriting from it, <c>Create</c> falls back to the non-optimized interface-based implementation.
/// </summary>
public static partial class ManualConsumatorTests
{
    public sealed class WithWrapper
    {
        private static ManualConsumator<int> Create(Conduit<int> conduit) =>
            ManualConsumator<int>.Create(new ConduitWrapper<int>(conduit));

        [Fact]
        public void TryGet_YieldsAllWrittenItemsInOrder()
        {
            var conduit = NewConduit();
            var consumator = Create(conduit);

            conduit.TryWrite(1);
            conduit.TryWrite(2);

            Assert.True(consumator.TryGet(out var first));
            consumator.Ack();
            Assert.True(consumator.TryGet(out var second));
            consumator.Ack();

            Assert.Equal([1, 2], [first, second]);
        }

        [Fact]
        public void TryGet_RepeatedWithoutAck_ReturnsSameItem()
        {
            var conduit = NewConduit();
            var consumator = Create(conduit);

            conduit.TryWrite(42);

            Assert.True(consumator.TryGet(out var first));
            Assert.True(consumator.TryGet(out var second));
            Assert.Equal(42, first);
            Assert.Equal(42, second);

            consumator.Ack();
            Assert.False(consumator.TryGet(out _));
        }

        [Fact]
        public void TryGet_WhenEmptyAndNotCompleted_ReturnsFalse()
        {
            var conduit = NewConduit();
            var consumator = Create(conduit);

            Assert.False(consumator.TryGet(out _));
        }

        [Fact]
        public void IsCompleted_ReflectsWrapperState()
        {
            var conduit = NewConduit();
            var consumator = Create(conduit);

            Assert.False(consumator.IsCompleted);

            conduit.TryComplete();

            Assert.True(consumator.IsCompleted);
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task OnReady_Fires_WhenDataBecomesAvailable()
        {
            var conduit = NewConduit();
            var consumator = Create(conduit);
            var ready = new TaskCompletionSource();

            consumator.OnReady += () => ready.TrySetResult();

            // Wake the consumer so it registers the pending wait on the EventTask.
            Assert.False(consumator.TryGet(out _));

            conduit.TryWrite(7);

            await ready.Task.WaitAsync(TimeSpan.FromMilliseconds(TimeoutMs), TestContext.Current.CancellationToken);
            Assert.True(consumator.TryGet(out var item));
            Assert.Equal(7, item);
        }
    }
}