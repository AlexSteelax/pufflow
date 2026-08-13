using System.Threading.Channels;
using JetBrains.Annotations;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Sdk.Test.SinkProcessors;

[Flow]
internal partial class FlowSinkProducator<T>
{
    private readonly Channel<T> _channel = Channel.CreateUnbounded<T>(new UnboundedChannelOptions
    {
        SingleReader = false,
        SingleWriter = true,
        AllowSynchronousContinuations = true
    });

    [PublicAPI]
    public void Fuse(out IAsyncProducator<T> target, FlowContext context)
    {
        target = new AsyncProducator(_channel, context);
    }

    [PublicAPI]
    public void Fuse(out IProducator<T> target, FlowContext context)
    {
        target = new AsyncProducator(_channel, context);
    }

    [PublicAPI]
    public ChannelReader<T> Reader => _channel.Reader;

    private sealed class AsyncProducator(ChannelWriter<T> writer, CancellationToken cancellationToken) : IAsyncProducator<T>
    {
        public bool TryWrite(T value)
        {
            return writer.TryWrite(value);
        }

        public bool TryComplete(Exception? ex = null)
        {
            return writer.TryComplete(ex);
        }

        public ValueTask<bool> WaitToWriteAsync() => writer.WaitToWriteAsync(cancellationToken);
    }
}
