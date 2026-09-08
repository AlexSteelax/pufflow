using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Steelax.Toolkit.HighPerformance;
using Steelax.Toolkit.HighPerformance.Concurrency.Collections;
using Steelax.Toolkit.HighPerformance.Concurrency.Primitives;

namespace Steelax.Pufflow.Operators.Internal;

[PublicAPI]
internal abstract class ManualConsumator<T>
{
    public abstract event Action OnReady;

    public abstract bool TryGet([MaybeNullWhen(false)] out T item);
    public abstract void Ack();
    public abstract bool IsCompleted { get; }
    public abstract bool IsOccupied { get; }

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
    
    private OccupiedItem<T> _occupied;
    
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
            _occupied = new OccupiedItem<T>(item);
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
        get => conduit.IsCompleted;
    }
    
    public override bool IsOccupied
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _occupied.Occupied;
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
    
    private OccupiedItem<T> _occupied;

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
                _occupied = new OccupiedItem<T>(item);
                return true;
            }

            if (consumator.IsCompleted)
                break;

            if (_input.GetState().IsPending)
                break;

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
        get => consumator.IsCompleted;
    }
    
    public override bool IsOccupied
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _occupied.Occupied;
    }
}

file readonly record struct OccupiedItem<T>
{
    public readonly T Value;
    public readonly bool Occupied;
    
    public OccupiedItem(T value)
    {
        Value = value;
        Occupied = true;
    }

    public OccupiedItem()
    {
        Value = default!;
        Occupied = false;
    }
}