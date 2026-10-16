using Steelax.Pufflow.Operators.Internal;
using Steelax.Toolkit.HighPerformance.Concurrency.Collections;

namespace Steelax.Pufflow.Operators.Aggregators.Buffering;

/// <summary>
///     A passive push→pull buffer bridge that decouples a push producer from a pull consumer over a single
///     bounded, lock-free SPSC channel.
/// </summary>
/// <typeparam name="T">The type of buffered values.</typeparam>
/// <remarks>
///     <para>
///         The component exposes both flow interfaces through one handler
///         (<c>Fuse(out IAsyncProducator{T}, out IAsyncConsumator{T}, ctx)</c>): the producator side is the
///         write endpoint an upstream push source pushes into (<c>TryWrite</c> / <c>WaitToWriteAsync</c>),
///         the consumator side is the read endpoint a downstream pull consumer reads from
///         (<c>TryRead</c> / <c>WaitToReadAsync</c>). Both sides wrap the same <see cref="Conduit{T}" />,
///         so values written on the producator side surface on the consumator side in FIFO order.
///     </para>
///     <para>
///         The buffer is bounded (<paramref name="capacity" />): when the channel is full, the producator
///         side applies backpressure by waiting in <c>WaitToWriteAsync</c> until the consumer drains space.
///     </para>
///     <para>
///         This is the connective layer between a push-style source (e.g. a Kafka consumer) and a
///         pull-style consumer (e.g. <c>WarmProcessor</c>), which cannot be connected directly because one
///         pushes and the other pulls. The component itself is passive — it holds no background task and
///         only relays values through the channel.
///     </para>
/// </remarks>
[Flow]
internal sealed partial class BypassBufferProcessor<T>(int capacity)
{
    /// <summary>The bounded SPSC channel shared by the producator (write) and consumator (read) sides.</summary>
    private readonly InternalConduit<T> _buffer = new(capacity, ConduitBehavior.AwaitableReader | ConduitBehavior.AwaitableWriter);

    [PublicAPI]
    public void Fuse(out IAsyncProducator<T> source, out IAsyncConsumator<T> target, FlowContext context)
    {
        Trace.WriteLine($"[BypassBufferProcessor] Fuse: buffer={GetHashCode()}");
        source = _buffer;
        target = _buffer;
        
        context.OnCancelled(token => _buffer.TryTerminate(new OperationCanceledException(token)));
    }
    
    [PublicAPI]
    public void Fuse(out IProducator<T> source, out IAsyncConsumator<T> target, FlowContext context)
    {
        Trace.WriteLine($"[BypassBufferProcessor] Fuse: buffer={GetHashCode()}");
        source = _buffer;
        target = _buffer;
        
        context.OnCancelled(token => _buffer.TryTerminate(new OperationCanceledException(token)));
    }
}