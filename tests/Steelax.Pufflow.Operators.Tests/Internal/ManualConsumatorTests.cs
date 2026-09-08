using System.Diagnostics.CodeAnalysis;
using Steelax.Pufflow.Operators.Internal;
using Steelax.Toolkit.HighPerformance.Concurrency.Collections;

namespace Steelax.Pufflow.Operators.Tests.Internal;

/// <summary>
///     Tests for <see cref="ManualConsumator{T}" /> factory dispatch and both consumption paths:
///     the optimized <c>ConduitManualConsumator</c> (when the source is a <see cref="Conduit{T}" />) and the
///     generic interface-based <c>SharedManualConsumator</c> (when the source is any other
///     <c>IAsyncConsumator{T}</c>, e.g. a wrapper around a conduit).
/// </summary>
public static partial class ManualConsumatorTests
{
    private const int TimeoutMs = 1_000;

    private static InternalConduit<int> NewConduit() =>
        new(capacity: 4, ConduitBehavior.AwaitableReader);

    /// <summary>
    ///     An <c>IAsyncConsumator{T}</c> that wraps a <see cref="Conduit{T}" /> without being a conduit itself,
    ///     forcing the generator path through the interface-based <c>SharedManualConsumator</c>.
    /// </summary>
    private sealed class ConduitWrapper<T>(Conduit<T> conduit) : Steelax.Pufflow.Abstractions.IAsyncConsumator<T>
    {
        public bool TryRead([MaybeNullWhen(false)] out T value)
        {
            if (conduit.TryRead(out value))
                return true;

            value = default!;
            return false;
        }

        public bool IsCompleted => conduit.IsCompleted;

        public ValueTask<bool> WaitToReadAsync() => conduit.WaitToReadAsync();
    }

    /// <summary>
    ///     Optimized path: an <c>InternalConduit{T}</c> (a <see cref="Conduit{T}" />) is dispatched by
    ///     <see cref="ManualConsumator{T}.Create" /> to the <c>ConduitManualConsumator</c>, which reads directly
    ///     through <c>TryRead</c> and the <c>OnReadReady</c> event, bypassing <c>WaitToReadAsync</c>.
    /// </summary>
    public sealed class WithConduit
    {
        [Fact]
        public void TryGet_YieldsAllWrittenItemsInOrder()
        {
            var conduit = NewConduit();
            var consumator = ManualConsumator<int>.Create(conduit);

            conduit.TryWrite(1);
            conduit.TryWrite(2);
            conduit.TryWrite(3);

            Assert.True(consumator.TryGet(out var first));
            consumator.Ack();
            Assert.True(consumator.TryGet(out var second));
            consumator.Ack();
            Assert.True(consumator.TryGet(out var third));
            consumator.Ack();

            Assert.Equal([1, 2, 3], [first, second, third]);
        }

        [Fact]
        public void TryGet_RepeatedWithoutAck_ReturnsSameItem()
        {
            var conduit = NewConduit();
            var consumator = ManualConsumator<int>.Create(conduit);

            conduit.TryWrite(42);

            Assert.True(consumator.TryGet(out var first));
            Assert.True(consumator.TryGet(out var second));

            Assert.Equal(42, first);
            Assert.Equal(42, second);

            consumator.Ack();
            Assert.False(consumator.TryGet(out _));
        }

        [Fact]
        public void IsCompleted_ReflectsConduitState()
        {
            var conduit = NewConduit();
            var consumator = ManualConsumator<int>.Create(conduit);

            Assert.False(consumator.IsCompleted);

            conduit.TryComplete();

            Assert.True(consumator.IsCompleted);
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task OnReady_Fires_WhenDataBecomesAvailable()
        {
            var conduit = NewConduit();
            var consumator = ManualConsumator<int>.Create(conduit);
            var ready = new TaskCompletionSource();

            consumator.OnReady += () => ready.TrySetResult();

            conduit.TryWrite(7);

            await ready.Task.WaitAsync(TimeSpan.FromMilliseconds(TimeoutMs), TestContext.Current.CancellationToken);
            Assert.True(consumator.TryGet(out var item));
            Assert.Equal(7, item);
        }
    }
}