using System.Runtime.CompilerServices;
using Steelax.Toolkit.HighPerformance;
using Steelax.Toolkit.HighPerformance.Concurrency.Collections;
using Steelax.Toolkit.HighPerformance.Concurrency.Primitives;

namespace Steelax.Pufflow.Operators.Internal;

[PublicAPI]
internal abstract class ManualProducator<T>
{
    public abstract event Action OnReady;

    public abstract bool TryWrite(T value);
    
    public abstract bool TryComplete(Exception? ex = null);
    
    public abstract bool IsFull { get; }
    
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
            
            if (_input.GetState().IsPending)
                break;

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
    public override bool TryComplete(Exception? ex = null)
    {
        return producator.TryComplete(ex);
    }

    public override bool IsFull
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => producator.IsFull;
    }
}