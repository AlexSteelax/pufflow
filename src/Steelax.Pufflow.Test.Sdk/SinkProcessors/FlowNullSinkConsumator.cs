using System.Diagnostics;
using System.Threading.Channels;
using JetBrains.Annotations;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Sdk.Test.SinkProcessors;

[Flow]
internal partial class FlowNullSinkConsumator<T>
{
    [PublicAPI]
    public void Fuse(IConsumator<T> source, FlowContext context)
    {
        context.RegisterBackground(() => ConsumeLoopAsync(source, context));
    }
    
    [PublicAPI]
    public void Fuse(IAsyncConsumator<T> source, FlowContext context)
    {
        Trace.WriteLine($"[FlowSinkConsumator] Fuse: source={source.GetHashCode()} type={source.GetType().Name}");
        context.RegisterBackground(() => ConsumeLoopAsync(source, context));
    }

    private static async Task ConsumeLoopAsync(IConsumator<T> source, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            if (source.TryRead(out _))
                continue;

            if (source.IsCompleted)
                break;

            await Task.Yield();
        }
    }
    
    private static async Task ConsumeLoopAsync(IAsyncConsumator<T> source, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            if (source.TryRead(out _))
                continue;

            if (!await source.WaitToReadAsync())
                break;
        }
    }
}