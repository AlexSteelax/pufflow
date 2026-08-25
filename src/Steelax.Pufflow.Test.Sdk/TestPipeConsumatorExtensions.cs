using JetBrains.Annotations;
using Steelax.Pufflow.Abstractions;
using Steelax.Pufflow.Sdk.Test.PipeProcessors;

namespace Steelax.Pufflow.Sdk.Test;

[PublicAPI]
public static class TestPipeConsumatorExtensions
{
    public static Source<IAsyncConsumator<T2>> ToAsyncConsumator<T1, T2>(this Source<IAsyncProducator<T1>> left, Func<T1, T2> selector)
    {
        var processor = new FlowPipeProducator<T1, T2>(selector);
        return left.Next(processor.FlowAProdToACons);
    }
    
    public static Source<IAsyncConsumator<T2>> ToAsyncConsumator<T1, T2>(this Source<IProducator<T1>> left, Func<T1, T2> selector)
    {
        var processor = new FlowPipeProducator<T1, T2>(selector);
        return left.Next(processor.FlowProdToACons);
    }
    
    public static Source<IConsumator<T2>> ToConsumator<T1, T2>(this Source<IAsyncProducator<T1>> left, Func<T1, T2> selector)
    {
        var processor = new FlowPipeProducator<T1, T2>(selector);
        return left.Next(processor.FlowAProdToCons);
    }
    
    public static Source<IConsumator<T2>> ToConsumator<T1, T2>(this Source<IProducator<T1>> left, Func<T1, T2> selector)
    {
        var processor = new FlowPipeProducator<T1, T2>(selector);
        return left.Next(processor.FlowProdToCons);
    }
    
    // #################
    
    public static Source<IAsyncConsumator<T2>> ToAsyncConsumator<T1, T2>(this Source<IAsyncConsumator<T1>> left, Func<T1, T2> selector)
    {
        var processor = new FlowPipeConsumator<T1, T2>(selector);
        return left.Next(processor.FlowAConsToACons);
    }
    
    public static Source<IAsyncConsumator<T2>> ToAsyncConsumator<T1, T2>(this Source<IConsumator<T1>> left, Func<T1, T2> selector)
    {
        var processor = new FlowPipeConsumator<T1, T2>(selector);
        return left.Next(processor.FlowConsToACons);
    }
    
    public static Source<IConsumator<T2>> ToConsumator<T1, T2>(this Source<IAsyncConsumator<T1>> left, Func<T1, T2> selector)
    {
        var processor = new FlowPipeConsumator<T1, T2>(selector);
        return left.Next(processor.FlowAConsToCons);
    }
    
    public static Source<IConsumator<T2>> ToConsumator<T1, T2>(this Source<IConsumator<T1>> left, Func<T1, T2> selector)
    {
        var processor = new FlowPipeConsumator<T1, T2>(selector);
        return left.Next(processor.FlowConsToCons);
    }
}