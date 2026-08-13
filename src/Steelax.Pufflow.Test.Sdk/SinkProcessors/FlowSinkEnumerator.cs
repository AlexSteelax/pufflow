using System.Threading.Channels;
using JetBrains.Annotations;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Sdk.Test.SinkProcessors;

[Flow]
internal partial class FlowSinkEnumerator<T>
{
    private readonly Channel<T> _channel = Channel.CreateUnbounded<T>(new UnboundedChannelOptions
    {
        SingleReader = false,
        SingleWriter = true,
        AllowSynchronousContinuations = true
    });
    
    [PublicAPI]
    public ChannelReader<T> Reader => _channel.Reader;
    
    [PublicAPI]
    public void Fuse(IAsyncEnumerator<T> source, FlowContext context)
    {
        context.RegisterBackground(() => ConsumeLoopAsync(source, _channel, context));
    }

    private static async Task ConsumeLoopAsync(IAsyncEnumerator<T> source, ChannelWriter<T> writer, CancellationToken cancellationToken)
    {
        try
        {
            while (await source.MoveNextAsync())
            {
                await writer.WriteAsync(source.Current, cancellationToken);
            }
        }
        catch (Exception ex)
        {
            writer.TryComplete(ex);
        }
        finally
        {
            writer.TryComplete();
        }
    }
}