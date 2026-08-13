using Steelax.Pufflow.Operators.Internal;
using Steelax.Toolkit.HighPerformance.Concurrency.Channels;

namespace Steelax.Pufflow.Operators.Aggregators.Buffering;

/// <summary>
///     A passive push→pull buffer bridge that decouples a push producer from a pull consumer over a single
///     bounded, lock-free SPSC channel.
/// </summary>
/// <typeparam name="T">The type of buffered values.</typeparam>
/// <remarks>
///     <para>
///         The component exposes both flow interfaces through one <see cref="Fuse" /> handler
///         (<c>Fuse(out IAsyncProducator{T}, out IAsyncConsumator{T}, ctx)</c>): the producator side is the
///         write endpoint an upstream push source pushes into (<c>TryWrite</c> / <c>WaitToWriteAsync</c>),
///         the consumator side is the read endpoint a downstream pull consumer reads from
///         (<c>TryRead</c> / <c>WaitToReadAsync</c>). Both sides wrap the same <see cref="SpscChannel{T}" />,
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
public sealed partial class BypassBufferProcessor<T>(int capacity)
{
    /// <summary>The bounded SPSC channel shared by the producator (write) and consumator (read) sides.</summary>
    private readonly SpscChannel<T> _buffer = new(capacity);
    
    /// <summary>
    ///     Hands out the two sides of the buffer: the push input producer (written by the upstream source)
    ///     and the pull output stream (read by the downstream consumer), both backed by the same channel.
    /// </summary>
    /// <param name="source">The push (producator) side of the buffer.</param>
    /// <param name="target">The pull (consumator) side of the buffer.</param>
    /// <param name="context">The flow context providing cancellation for the pipeline.</param>
    [PublicAPI]
    public void Fuse(out IAsyncProducator<T> source, out IAsyncConsumator<T> target, FlowContext context)
    {
        var channel = new InternalSpscChannel<T>(_buffer);
        Trace.WriteLine($"[BypassBufferProcessor] Fuse: channel={channel.GetHashCode()} buffer={_buffer.GetHashCode()}");
        source = channel;
        target = channel;
    }
}
