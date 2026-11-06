using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Sdk.Test.SourceProcessors;

/// <summary>
///     An asynchronous pull source over a <see cref="Channel{T}" />: exposes the channel's reader as an
///     <see cref="IAsyncConsumator{T}" />.
/// </summary>
/// <typeparam name="T">The element type.</typeparam>
[Flow]
internal sealed partial class FlowAsyncConsumatorSource<T>
{
    /// <summary>Initializes the source against the channel and hands out its async pull reader.</summary>
    /// <param name="channel">The channel whose reader is exposed through the flow.</param>
    /// <param name="output">The asynchronous pull reader handed to the downstream chain.</param>
    /// <param name="_">The flow context (unused by this source).</param>
    public FlowAsyncConsumatorSource(Channel<T> channel, out IAsyncConsumator<T> output, FlowContext _)
    {
        output = new AsyncConsumator(channel.Reader);
    }

    private sealed class AsyncConsumator(ChannelReader<T> reader) : IAsyncConsumator<T>
    {
        public bool TryRead([MaybeNullWhen(false)] out T value) => reader.TryRead(out value);

        public bool IsCompleted => reader.Completion.IsCompleted;

        public ValueTask<bool> WaitToReadAsync() => reader.WaitToReadAsync();
    }
}