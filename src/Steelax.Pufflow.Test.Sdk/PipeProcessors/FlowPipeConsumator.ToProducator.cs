using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Sdk.Test.PipeProcessors;

internal sealed partial class FlowPipeConsumator<TSource, TTarget>
{
    public void Fuse(IAsyncConsumator<TSource> input, IProducator<TTarget> output, FlowContext context)
    {
        var pipe = new AsyncConsumatorToAsyncProducator<IAsyncConsumator<TSource>, IProducator<TTarget>>(input, output, selector, context);
        context.RegisterBackground(pipe.ExecuteAsync);
    }
    
    public void Fuse(IConsumator<TSource> input, IProducator<TTarget> output, FlowContext context)
    {
        var pipe = new AsyncConsumatorToAsyncProducator<IConsumator<TSource>, IProducator<TTarget>>(input, output, selector, context);
        context.RegisterBackground(pipe.ExecuteAsync);
    }
}