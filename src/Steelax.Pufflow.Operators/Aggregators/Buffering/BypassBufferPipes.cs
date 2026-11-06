using Steelax.Pufflow.Operators.Internal;
using Steelax.Toolkit.HighPerformance.Concurrency.Collections;

namespace Steelax.Pufflow.Operators.Aggregators.Buffering;

/// <summary>Async push → async pull passive buffer bridge.</summary>
[Flow]
internal sealed partial class BypassBufferAProdToACons<T>
{
    private readonly InternalConduit<T> _buffer;

    public BypassBufferAProdToACons(int capacity,
        out IAsyncProducator<T> producer, out IAsyncConsumator<T> target, FlowContext context)
    {
        _buffer = new InternalConduit<T>(capacity, ConduitBehavior.AwaitableReader | ConduitBehavior.AwaitableWriter);
        producer = _buffer;
        target = _buffer;

        context.OnCancelled(token => _buffer.TryTerminate(new OperationCanceledException(token)));
    }
}

/// <summary>Sync push → async pull passive buffer bridge.</summary>
[Flow]
internal sealed partial class BypassBufferProdToACons<T>
{
    private readonly InternalConduit<T> _buffer;

    public BypassBufferProdToACons(int capacity,
        out IProducator<T> producer, out IAsyncConsumator<T> target, FlowContext context)
    {
        _buffer = new InternalConduit<T>(capacity, ConduitBehavior.AwaitableReader | ConduitBehavior.AwaitableWriter);
        producer = _buffer;
        target = _buffer;

        context.OnCancelled(token => _buffer.TryTerminate(new OperationCanceledException(token)));
    }
}