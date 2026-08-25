using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Sdk.Test.PipeProcessors;

internal sealed partial class FlowPipeProducator<TSource, TTarget>
{
    public void Fuse(out IAsyncProducator<TSource> source, out IConsumator<TTarget> target, FlowContext context)
    {
        var pipe = new AsyncProducatorToAsyncConsumator(selector, context);
        source = pipe;
        target = pipe;
    }
    
    public void Fuse(out IProducator<TSource> source, out IConsumator<TTarget> target, FlowContext context)
    {
        var pipe = new AsyncProducatorToAsyncConsumator(selector, context);
        source = pipe;
        target = pipe;
    }
}