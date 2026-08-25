using JetBrains.Annotations;
using Steelax.Pufflow.Abstractions;
using Steelax.Pufflow.Sdk.Test.PipeProcessors;

namespace Steelax.Pufflow.Sdk.Test;

[PublicAPI]
public static class TestPipeProducatorExtensions
{
    public static Source<IAsyncProducator<T2>> ToAsyncProducator<T1, T2>(this Source<IAsyncProducator<T1>> left, Func<T1, T2> selector)
    {
        var processor = new FlowPipeProducator<T1, T2>(selector);
        return left.Next(processor.FlowAProdToAProd);
    }
    
    public static Source<IAsyncProducator<T2>> ToAsyncProducator<T1, T2>(this Source<IProducator<T1>> left, Func<T1, T2> selector)
    {
        var processor = new FlowPipeProducator<T1, T2>(selector);
        return left.Next(processor.FlowProdToAProd);
    }
    
    public static Source<IProducator<T2>> ToProducator<T1, T2>(this Source<IAsyncProducator<T1>> left, Func<T1, T2> selector)
    {
        var processor = new FlowPipeProducator<T1, T2>(selector);
        return left.Next(processor.FlowAProdToProd);
    }
    
    public static Source<IProducator<T2>> ToProducator<T1, T2>(this Source<IProducator<T1>> left, Func<T1, T2> selector)
    {
        var processor = new FlowPipeProducator<T1, T2>(selector);
        return left.Next(processor.FlowProdToProd);
    }
    
    // #################
    
    public static Source<IAsyncProducator<T2>> ToAsyncProducator<T1, T2>(this Source<IAsyncConsumator<T1>> left, Func<T1, T2> selector)
    {
        var processor = new FlowPipeConsumator<T1, T2>(selector);
        return left.Next(processor.FlowAConsToAProd);
    }
    
    public static Source<IAsyncProducator<T2>> ToAsyncProducator<T1, T2>(this Source<IConsumator<T1>> left, Func<T1, T2> selector)
    {
        var processor = new FlowPipeConsumator<T1, T2>(selector);
        return left.Next(processor.FlowConsToAProd);
    }
    
    public static Source<IProducator<T2>> ToProducator<T1, T2>(this Source<IAsyncConsumator<T1>> left, Func<T1, T2> selector)
    {
        var processor = new FlowPipeConsumator<T1, T2>(selector);
        return left.Next(processor.FlowAConsToProd);
    }
    
    public static Source<IProducator<T2>> ToProducator<T1, T2>(this Source<IConsumator<T1>> left, Func<T1, T2> selector)
    {
        var processor = new FlowPipeConsumator<T1, T2>(selector);
        return left.Next(processor.FlowConsToProd);
    }
}