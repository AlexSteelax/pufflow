using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Sdk.Test.PipeProcessors;

internal sealed partial class FlowPipeProducator<TSource, TTarget>
{
    public void Fuse(out IAsyncProducator<TSource> source, IProducator<TTarget> target, FlowContext context)
    {
        var pipe = new AsyncProducatorToAsyncProducator<IProducator<TTarget>>(target, selector, context);
        context.RegisterBackground(pipe.ExecuteAsync);
        source = pipe;
    }
    
    public void Fuse(out IProducator<TSource> source, IProducator<TTarget> target, FlowContext context)
    {
        var pipe = new AsyncProducatorToAsyncProducator<IProducator<TTarget>>(target, selector, context);
        context.RegisterBackground(pipe.ExecuteAsync);
        source = pipe;
    }
}