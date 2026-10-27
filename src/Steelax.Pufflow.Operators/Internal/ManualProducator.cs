using System.Runtime.CompilerServices;
using Steelax.Toolkit.HighPerformance;
using Steelax.Toolkit.HighPerformance.Concurrency.Collections;
using Steelax.Toolkit.HighPerformance.Concurrency.Primitives;

namespace Steelax.Pufflow.Operators.Internal;

/// <summary>
///     A push-side bridge that writes elements into an asynchronous push target with write-side backpressure
///     handling: <see cref="TryWrite" /> retries internally through the target's <c>WaitToWriteAsync</c> readiness
///     signal until the element is accepted or the stream ends. Synchronous workers use it to fan into an
///     asynchronous producer without managing the wait themselves.
/// </summary>
/// <typeparam name="T">The element type.</typeparam>
[PublicAPI]
public abstract class ManualProducator<T>
{
    /// <summary>
    ///     Raised when the underlying target reports write-readiness (capacity available) or reaches a terminal
    ///     state. Subscribe once, before the first <see cref="TryWrite" />.
    /// </summary>
    public abstract event Action OnReady;

    /// <summary>
    ///     Writes an element, waiting (observing the target's readiness signal) until the target accepts it.
    /// </summary>
    /// <param name="value">The element to write.</param>
    /// <returns><see langword="true" /> when the element was accepted; <see langword="false" /> if the stream ended.</returns>
    public abstract bool TryWrite(T value);

    /// <summary>
    ///     Completes the underlying target, optionally faulting it with <paramref name="ex" />.
    /// </summary>
    /// <param name="ex">The exception surfacing to the reader, if the stream is faulted.</param>
    /// <returns><see langword="true" /> when the target was completed by this call.</returns>
    public abstract bool TryComplete(Exception? ex = null);

    /// <summary>Gets whether the underlying target is currently not accepting elements.</summary>
    public abstract bool IsFull { get; }

    /// <summary>
    ///     Creates a <see cref="ManualProducator{T}" /> over the specified asynchronous push target. A conduit
    ///     target maps to a direct wrapper; any other target is wrapped generically, bridging its
    ///     <c>WaitToWriteAsync</c> into the <see cref="OnReady" /> event.
    /// </summary>
    /// <param name="producator">The asynchronous push target to write into.</param>
    /// <returns>A manual wrapper over the target.</returns>
    public static ManualProducator<T> Create(IAsyncProducator<T> producator)
    {
        if (producator is Conduit<T> conduit)
            return new ConduitManualProducator<T>(conduit);
        
        return CreateAsGeneric(producator);
    }
    
    private static ManualProducator<T> CreateAsGeneric(IAsyncProducator<T> consumator)
    {
        var genericType = typeof(SharedManualProducator<,>).MakeGenericType(typeof(T), consumator.GetType());
        return (ManualProducator<T>)Activator.CreateInstance(genericType, consumator)!;
    }
}

file sealed class ConduitManualProducator<T>(Conduit<T> conduit) : ManualProducator<T>
{
    public override event Action? OnReady
    {
        add => conduit.OnWriteReady += value;
        remove => conduit.OnWriteReady -= value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool TryWrite(T value)
    {
        return conduit.TryWrite(value);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool TryComplete(Exception? ex = null)
    {
        return conduit.TryComplete(ex);
    }


    public override bool IsFull
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => conduit.IsFull;
    }
}

file sealed class SharedManualProducator<T, TProducator>(TProducator producator) : ManualProducator<T>
    where TProducator : IAsyncProducator<T>
{
    private readonly EventTask<bool> _input = new();
    
    public override event Action? OnReady
    {
        add => _input.OnReady += value;
        remove => _input.OnReady -= value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool TryWrite(T value)
    {
        while (true)
        {
            if (producator.TryWrite(value))
                return true;

            var state = _input.GetState();

            if (state.IsPending)
                return false;

            if (state.IsCompleted)
            {
                if (!_input.GetResult())
                    return false;
            }

            if (_input.Observe(producator.WaitToWriteAsync(), OnCompletedBehavior.SkipCallbackIfCompleted))
            {
                if (_input.GetResult())
                    continue;
            }

            break;
        }
        
        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool TryComplete(Exception? ex = null) => producator.TryComplete(ex);

    public override bool IsFull
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => producator.IsFull;
    }
}