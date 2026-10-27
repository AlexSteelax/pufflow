using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Steelax.Pufflow.Operators.Common;
using Steelax.Toolkit.HighPerformance;
using Steelax.Toolkit.HighPerformance.Concurrency.Collections;
using Steelax.Toolkit.HighPerformance.Concurrency.Primitives;

namespace Steelax.Pufflow.Operators.Internal;

/// <summary>
///     A pull-side bridge that fetches elements from an asynchronous pull source one at a time, with an explicit
///     two-phase handshake: <see cref="TryGet" /> makes the element available for processing and
///     <see cref="Ack" /> releases it once consumed. This lets a synchronous worker read the element, keep it in a
///     local slot between calls, and acknowledge it at its own pace without losing or duplicating values.
/// </summary>
/// <typeparam name="T">The element type.</typeparam>
[PublicAPI]
public abstract class ManualConsumator<T>
{
    /// <summary>
    ///     Raised when the underlying source reports read-readiness (a value is available) or reaches a terminal
    ///     state. Subscribe once, before the first <see cref="TryGet" />.
    /// </summary>
    public abstract event Action OnReady;

    /// <summary>
    ///     Fetches the next element without consuming the stream position: the fetched element stays in the slot
    ///     until <see cref="Ack" /> releases it, so repeated calls between acknowledges return the same element.
    /// </summary>
    /// <param name="item">The fetched element, when one is available and not yet acknowledged.</param>
    /// <returns><see langword="true" /> when an element is available; otherwise <see langword="false" />.</returns>
    public abstract bool TryGet([MaybeNullWhen(false)] out T item);

    /// <summary>Releases the currently fetched element, allowing the stream to advance to the next one.</summary>
    public abstract void Ack();

    /// <summary>
    ///     Gets whether the underlying source has ended and no unacknowledged element remains in the slot.
    /// </summary>
    public abstract bool IsCompleted { get; }

    /// <summary>
    ///     Creates a <see cref="ManualConsumator{T}" /> over the specified asynchronous pull source. A conduit
    ///     source maps to a direct wrapper; any other source is wrapped generically, bridging its
    ///     <c>WaitToReadAsync</c> into the <see cref="OnReady" /> event.
    /// </summary>
    /// <param name="consumator">The asynchronous pull source to fetch from.</param>
    /// <returns>A manual fetch-and-ack wrapper over the source.</returns>
    public static ManualConsumator<T> Create(IAsyncConsumator<T> consumator)
    {
        if (consumator is Conduit<T> conduit)
            return new ConduitManualConsumator<T>(conduit);
        
        return CreateAsGeneric(consumator);
    }

    private static ManualConsumator<T> CreateAsGeneric(IAsyncConsumator<T> consumator)
    {
        var genericType = typeof(SharedManualConsumator<,>).MakeGenericType(typeof(T), consumator.GetType());
        return (ManualConsumator<T>)Activator.CreateInstance(genericType, consumator)!;
    }
}

file sealed class ConduitManualConsumator<T>(Conduit<T> conduit) : ManualConsumator<T>
{
    public override event Action? OnReady
    {
        add => conduit.OnReadReady += value;
        remove => conduit.OnReadReady -= value;
    }
    
    private PendingValue<T> _occupied;
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool TryGet([MaybeNullWhen(false)] out T item)
    {
        if (_occupied.Occupied)
        {
            item = _occupied.Value;
            return true;
        }
        
        if (conduit.TryRead(out item))
        {
            _occupied = new PendingValue<T>(item);
            return true;
        }

        item = default;
        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override void Ack()
    {
        _occupied = default;
    }

    public override bool IsCompleted
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => conduit.IsCompleted && !_occupied.Occupied;
    }
}

file sealed class SharedManualConsumator<T, TConsumator>(TConsumator consumator) : ManualConsumator<T>
    where TConsumator : IAsyncConsumator<T>
{
    private readonly EventTask<bool> _input = new();
    
    public override event Action? OnReady
    {
        add => _input.OnReady += value;
        remove => _input.OnReady -= value;
    }
    
    private PendingValue<T> _occupied;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool TryGet([MaybeNullWhen(false)] out T item)
    {
        if (_occupied.Occupied)
        {
            item = _occupied.Value;
            return true;
        }
        
        while (true)
        {
            if (consumator.TryRead(out item))
            {
                _occupied = new PendingValue<T>(item);
                return true;
            }
            
            var state = _input.GetState();

            if (state.IsPending)
                break;

            if (state.IsCompleted)
            {
                if (!_input.GetResult())
                    break;
            }
            
            if (_input.Observe(consumator.WaitToReadAsync(), OnCompletedBehavior.SkipCallbackIfCompleted))
            {
                if (_input.GetResult())
                    continue;
            }

            break;
        }

        item = default;
        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override void Ack()
    {
        _occupied = default;
    }
    
    public override bool IsCompleted
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => consumator.IsCompleted && !_occupied.Occupied;
    }
}
