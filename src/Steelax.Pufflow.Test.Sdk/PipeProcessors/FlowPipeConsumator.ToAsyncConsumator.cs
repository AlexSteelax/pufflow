using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Sdk.Test.PipeProcessors;

internal sealed partial class FlowPipeConsumator<TSource, TTarget>
{
    public void Fuse(IAsyncConsumator<TSource> source, out IAsyncConsumator<TTarget> target, FlowContext context)
    {
        var pipe = new AsyncConsumatorToAsyncConsumator<IAsyncConsumator<TSource>>(source, selector, context);
        context.RegisterBackground(pipe.ExecuteAsync);
        target = pipe;
    }
    
    public void Fuse(IConsumator<TSource> source, out IAsyncConsumator<TTarget> target, FlowContext context)
    {
        var pipe = new AsyncConsumatorToAsyncConsumator<IConsumator<TSource>>(source, selector, context);
        context.RegisterBackground(pipe.ExecuteAsync);
        target = pipe;
    }
}