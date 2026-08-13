using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using JetBrains.Annotations;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Sdk.Test.SourceProcessors;

[Flow]
internal partial class FlowSourceConsumator<T>
{
    private readonly Channel<T> _channel = Channel.CreateUnbounded<T>(new UnboundedChannelOptions
    {
        SingleReader = true,
        SingleWriter = false,
        AllowSynchronousContinuations = true
    });
    
    [PublicAPI]
    public ChannelWriter<T> Writer => _channel.Writer;
    
    [PublicAPI]
    public void Fuse(out IConsumator<T> source, FlowContext context)
    {
        source = new AsyncConsumator(_channel.Reader, context);
    }
    
    [PublicAPI]
    public void Fuse(out IAsyncConsumator<T> source, FlowContext context)
    {
        source = new AsyncConsumator(_channel.Reader, context);
    }

    private sealed class AsyncConsumator(ChannelReader<T> reader, CancellationToken cancellationToken) : IAsyncConsumator<T>
    {
        public bool TryRead([MaybeNullWhen(false)] out T value)
        {
            return reader.TryRead(out value);
        }

        public bool IsCompleted => reader.Completion.IsCompleted;
        
        public ValueTask<bool> WaitToReadAsync() => reader.WaitToReadAsync(cancellationToken);
    }
}