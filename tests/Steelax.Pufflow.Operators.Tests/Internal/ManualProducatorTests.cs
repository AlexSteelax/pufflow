using Steelax.Pufflow.Operators.Internal;
using Steelax.Toolkit.HighPerformance.Concurrency.Collections;

namespace Steelax.Pufflow.Operators.Tests.Internal;

/// <summary>
///     Tests for <see cref="ManualProducator{T}" /> factory dispatch and both writing paths:
///     the optimized <c>ConduitManualProducator</c> (when the source is a <see cref="Conduit{T}" />) and the
///     generic interface-based <c>SharedManualProducator</c> (when the source is any other
///     <c>IAsyncProducator{T}</c>, e.g. a wrapper around a conduit).
/// </summary>
public static partial class ManualProducatorTests
{
    private const int TimeoutMs = 1_000;

    private static InternalConduit<int> NewConduit() =>
        new(capacity: 4, ConduitBehavior.AwaitableWriter);

    /// <summary>
    ///     An <c>IAsyncProducator{T}</c> that wraps a <see cref="Conduit{T}" /> without being a conduit itself,
    ///     forcing the generator path through the interface-based <c>SharedManualProducator</c>.
    /// </summary>
    private sealed class ConduitWrapper<T>(Conduit<T> conduit) : Steelax.Pufflow.Abstractions.IAsyncProducator<T>
    {
        public bool TryWrite(T value) => conduit.TryWrite(value);

        public bool TryComplete(Exception? ex = null) => conduit.TryComplete(ex);

        public bool IsFull => conduit.IsFull;

        public ValueTask<bool> WaitToWriteAsync() => conduit.WaitToWriteAsync();
    }

    /// <summary>
    ///     Optimized path: an <c>InternalConduit{T}</c> (a <see cref="Conduit{T}" />) is dispatched by
    ///     <see cref="ManualProducator{T}.Create" /> to the <c>ConduitManualProducator</c>, which writes directly
    ///     through <c>TryWrite</c> and the <c>OnWriteReady</c> event, bypassing <c>WaitToWriteAsync</c>.
    /// </summary>
    public sealed class WithConduit
    {
        [Fact]
        public void TryWrite_AcceptsItemsInOrder()
        {
            var conduit = NewConduit();
            var producator = ManualProducator<int>.Create(conduit);

            Assert.True(producator.TryWrite(1));
            Assert.True(producator.TryWrite(2));
            Assert.True(producator.TryWrite(3));

            Assert.True(conduit.TryRead(out var first));
            Assert.True(conduit.TryRead(out var second));
            Assert.True(conduit.TryRead(out var third));

            Assert.Equal([1, 2, 3], [first, second, third]);
        }

        [Fact]
        public void TryWrite_WhenFull_ReturnsFalse()
        {
            var conduit = NewConduit();
            var producator = ManualProducator<int>.Create(conduit);

            Assert.True(producator.TryWrite(1));
            Assert.True(producator.TryWrite(2));
            Assert.True(producator.TryWrite(3));
            Assert.True(producator.TryWrite(4));

            Assert.True(producator.IsFull);
            Assert.False(producator.TryWrite(5));
        }

        [Fact]
        public void TryComplete_ReflectsConduitState()
        {
            var conduit = NewConduit();
            var producator = ManualProducator<int>.Create(conduit);

            Assert.False(conduit.IsCompleted);

            Assert.True(producator.TryComplete());

            Assert.True(conduit.IsCompleted);
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task OnReady_Fires_WhenSpaceBecomesAvailable()
        {
            var conduit = NewConduit();
            var producator = ManualProducator<int>.Create(conduit);
            var ready = new TaskCompletionSource();

            producator.OnReady += () => ready.TrySetResult();

            // Fill the conduit so the next write is rejected.
            Assert.True(producator.TryWrite(1));
            Assert.True(producator.TryWrite(2));
            Assert.True(producator.TryWrite(3));
            Assert.True(producator.TryWrite(4));
            Assert.False(producator.TryWrite(5));

            // Drain one slot to free capacity and fire OnWriteReady.
            Assert.True(conduit.TryRead(out _));

            await ready.Task.WaitAsync(TimeSpan.FromMilliseconds(TimeoutMs), TestContext.Current.CancellationToken);
            Assert.True(producator.TryWrite(6));
        }
    }
}