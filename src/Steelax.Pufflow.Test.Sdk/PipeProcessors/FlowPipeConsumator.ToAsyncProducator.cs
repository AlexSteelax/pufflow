using System.Threading.Channels;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Sdk.Test.PipeProcessors;

internal sealed partial class FlowPipeConsumator<TSource, TTarget>
{
    public void Fuse(IAsyncConsumator<TSource> input, IAsyncProducator<TTarget> output, FlowContext context)
    {
        var pipe = new AsyncConsumatorToAsyncProducator<IAsyncConsumator<TSource>, IAsyncProducator<TTarget>>(input, output, selector, context);
        context.RegisterBackground(pipe.ExecuteAsync);
    }
    
    public void Fuse(IConsumator<TSource> input, IAsyncProducator<TTarget> output, FlowContext context)
    {
        var pipe = new AsyncConsumatorToAsyncProducator<IConsumator<TSource>, IAsyncProducator<TTarget>>(input, output, selector, context);
        context.RegisterBackground(pipe.ExecuteAsync);
    }
}