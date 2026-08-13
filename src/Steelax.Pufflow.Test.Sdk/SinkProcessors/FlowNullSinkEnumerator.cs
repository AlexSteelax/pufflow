using JetBrains.Annotations;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Sdk.Test.SinkProcessors;

[Flow]
internal partial class FlowNullSinkEnumerator<T>
{
    [PublicAPI]
    public void Fuse(IAsyncEnumerator<T> source, FlowContext context)
    {
        context.RegisterBackground(() => ConsumeLoopAsync(source));
    }

    private static async Task ConsumeLoopAsync(IAsyncEnumerator<T> source)
    {
        while (await source.MoveNextAsync()) ;
    }
}