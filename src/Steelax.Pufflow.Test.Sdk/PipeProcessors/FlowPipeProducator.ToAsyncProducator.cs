using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Sdk.Test.PipeProcessors;

internal sealed partial class FlowPipeProducator<TSource, TTarget>
{
    public void Fuse(out IAsyncProducator<TSource> source, IAsyncProducator<TTarget> target, FlowContext context)
    {
        var pipe = new AsyncProducatorToAsyncProducator<IAsyncProducator<TTarget>>(target, selector, context);
        context.RegisterBackground(pipe.ExecuteAsync);
        source = pipe;
    }
    
    public void Fuse(out IProducator<TSource> source, IAsyncProducator<TTarget> target, FlowContext context)
    {
        var pipe = new AsyncProducatorToAsyncProducator<IAsyncProducator<TTarget>>(target, selector, context);
        context.RegisterBackground(pipe.ExecuteAsync);
        source = pipe;
    }
}