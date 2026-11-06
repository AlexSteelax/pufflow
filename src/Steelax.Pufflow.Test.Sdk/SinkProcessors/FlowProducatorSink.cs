using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Sdk.Test.SinkProcessors;

/// <summary>
///     A synchronous push sink over a <see cref="Channel{T}" />: hands out a synchronous target that
///     writes into the channel.
/// </summary>
/// <typeparam name="T">The element type.</typeparam>
[Flow]
internal sealed partial class FlowProducatorSink<T>
{
    /// <summary>Initializes the sink and hands out its write target.</summary>
    /// <param name="channel">The channel written by the sink.</param>
    /// <param name="writer">The synchronous push target handed to the upstream chain.</param>
    /// <param name="_">The flow context (unused by this sink).</param>
    public FlowProducatorSink(Channel<T> channel, out IProducator<T> writer, FlowContext _)
    {
        writer = new Producator(channel.Writer);
    }

    private sealed class Producator(ChannelWriter<T> writer) : IProducator<T>
    {
        public bool TryWrite(T value) => writer.TryWrite(value);

        public bool TryComplete(Exception? ex = null) => writer.TryComplete(ex);

        public bool IsFull => false;
    }
}