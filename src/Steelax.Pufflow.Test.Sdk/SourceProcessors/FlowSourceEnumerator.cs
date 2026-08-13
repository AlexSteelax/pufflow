using System.Threading.Channels;
using JetBrains.Annotations;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Sdk.Test.SourceProcessors;

[Flow]
internal partial class FlowSourceEnumerator<T>
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
    public void Fuse(out IAsyncEnumerator<T> source, FlowContext context)
    {
        source = Enumerate(context);
    }

    private async IAsyncEnumerator<T> Enumerate(FlowContext context)
    {
        var reader = _channel.Reader;
        var token = context.Token;
        
        while (!token.IsCancellationRequested)
        {
            if (reader.TryPeek(out var item))
            {
                yield return item;
                _ = reader.TryRead(out _);
                continue;
            }

            if (!await reader.WaitToReadAsync(token))
                break;
        }
    }
}