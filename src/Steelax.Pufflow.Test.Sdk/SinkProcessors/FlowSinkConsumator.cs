using System.Diagnostics;
using System.Threading.Channels;
using JetBrains.Annotations;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Sdk.Test.SinkProcessors;

[Flow]
internal partial class FlowSinkConsumator<T>
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
    public void Fuse(IConsumator<T> source, FlowContext context)
    {
        Trace.WriteLine($"[FlowSinkConsumator] Fuse: source={source.GetHashCode()} type={source.GetType().Name}");
        context.RegisterBackground(() => ConsumeLoopAsync(source, _channel, context));
    }
    
    [PublicAPI]
    public void Fuse(IAsyncConsumator<T> source, FlowContext context)
    {
        Trace.WriteLine($"[FlowSinkConsumator] Fuse: source={source.GetHashCode()} type={source.GetType().Name}");
        context.RegisterBackground(() => ConsumeLoopAsync(source, _channel, context));
    }

    private static async Task ConsumeLoopAsync(IConsumator<T> source, ChannelWriter<T> writer, CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                if (source.TryRead(out var item))
                {
                    await writer.WriteAsync(item, cancellationToken);
                    continue;
                }

                if (source.IsCompleted)
                    break;

                await Task.Yield();
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
    
    private static async Task ConsumeLoopAsync(IAsyncConsumator<T> source, ChannelWriter<T> writer, CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                if (source.TryRead(out var item))
                {
                    await writer.WriteAsync(item, cancellationToken);
                    continue;
                }

                if (!await source.WaitToReadAsync())
                    break;
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