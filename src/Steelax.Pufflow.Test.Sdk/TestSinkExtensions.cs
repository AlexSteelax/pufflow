using System.Threading.Channels;
using JetBrains.Annotations;
using Steelax.Pufflow.Abstractions;
using Steelax.Pufflow.Sdk.Test.SinkProcessors;

namespace Steelax.Pufflow.Sdk.Test;

[PublicAPI]
public static class TestSinkExtensions
{
    [PublicAPI]
    public static Sink<IAsyncConsumator<T>> Consume<T>(this Source<IAsyncConsumator<T>> left, out ChannelReader<T> reader)
    {
        var right = new FlowSinkConsumator<T>();
        reader = right.Reader;
        return left.End(right);
    }
    
    [PublicAPI]
    public static Sink<IAsyncConsumator<T>> Consume<T>(this Source<IAsyncConsumator<T>> left)
    {
        var right = new FlowNullSinkConsumator<T>();
        return left.End(right);
    }
    
    [PublicAPI]
    public static Sink<IAsyncEnumerator<T>> Consume<T>(this Source<IAsyncEnumerator<T>> left, out ChannelReader<T> reader)
    {
        var right = new FlowSinkEnumerator<T>();
        reader = right.Reader;
        return left.End(right);
    }
    
    [PublicAPI]
    public static Sink<IAsyncEnumerator<T>> Consume<T>(this Source<IAsyncEnumerator<T>> left)
    {
        var right = new FlowNullSinkEnumerator<T>();
        return left.End(right);
    }
    
    [PublicAPI]
    public static Sink<IAsyncProducator<T>> Consume<T>(this Source<IAsyncProducator<T>> left, out ChannelReader<T> reader)
    {
        var right = new FlowSinkProducator<T>();
        reader = right.Reader;
        return left.End(right.FlowAProd);
    }
    
    [PublicAPI]
    public static Sink<IAsyncProducator<T>> Consume<T>(this Source<IAsyncProducator<T>> left)
    {
        var right = new FlowNullSinkProducator<T>();
        return left.End(right.FlowAProd);
    }
    
    [PublicAPI]
    public static Sink<IProducator<T>> Consume<T>(this Source<IProducator<T>> left, out ChannelReader<T> reader)
    {
        var right = new FlowSinkProducator<T>();
        reader = right.Reader;
        return left.End(right.FlowProd);
    }
    
    [PublicAPI]
    public static Sink<IProducator<T>> Consume<T>(this Source<IProducator<T>> left)
    {
        var right = new FlowNullSinkProducator<T>();
        return left.End(right.FlowProd);
    }
}