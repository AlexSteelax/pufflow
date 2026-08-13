using System.Diagnostics;
using System.Threading.Channels;
using JetBrains.Annotations;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Sdk.Test.SourceProcessors;

[Flow]
internal partial class FlowSourceProducator<T>
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
    public void Fuse(IAsyncProducator<T> target, FlowContext context)
    {
        Trace.WriteLine($"[FlowSourceProducator] Fuse: target={target.GetHashCode()} type={target.GetType().Name}");
        context.RegisterBackground(() => ProduceLoopAsync(_channel, target, context));
    }

    [PublicAPI]
    public void Fuse(IProducator<T> target, FlowContext context)
    {
        context.RegisterBackground(() => ProduceLoopAsync(_channel, target, context));
    }
    
    private static async Task ProduceLoopAsync(ChannelReader<T> reader, IProducator<T> output, CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                if (reader.TryPeek(out var item))
                {
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        if (output.TryWrite(item))
                        {
                            _ = reader.TryRead(out _);
                            break;
                        }
                        
                        Thread.Yield();
                    }

                    continue;
                }

                if (!await reader.WaitToReadAsync(cancellationToken))
                    break;
            }
        }
        finally
        {
            output.TryComplete();
        }
    }
    
    private static async Task ProduceLoopAsync(ChannelReader<T> reader, IAsyncProducator<T> output, CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                if (reader.TryPeek(out var item))
                {
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        if (output.TryWrite(item))
                        {
                            _ = reader.TryRead(out _);
                            break;
                        }
                        
                        await output.WaitToWriteAsync();
                    }

                    continue;
                }

                if (!await reader.WaitToReadAsync(cancellationToken))
                    break;
            }
        }
        finally
        {
            output.TryComplete();
        }
    }
}