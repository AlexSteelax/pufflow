using System.Threading.Channels;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Sdk.Test.PipeProcessors;

/// <summary>A synchronous push → synchronous pull (composite) map pipe.</summary>
[Flow]
internal sealed partial class MapProducatorToConsumator<T1, T2>
{
    public MapProducatorToConsumator(Func<T1, T2> selector, out IProducator<T1> producer, out IConsumator<T2> output, FlowContext _)
    {
        var channel = Channel.CreateBounded<T1>(2);
        producer = new MapSupport.SyncWriter<T1>(channel.Writer);
        output = new MapSupport.MapReader<T1, T2>(channel.Reader, selector);
    }
}

/// <summary>A synchronous push → asynchronous pull (composite) map pipe.</summary>
[Flow]
internal sealed partial class MapProducatorToAsyncConsumator<T1, T2>
{
    public MapProducatorToAsyncConsumator(Func<T1, T2> selector, out IProducator<T1> producer, out IAsyncConsumator<T2> output, FlowContext _)
    {
        var channel = Channel.CreateBounded<T1>(2);
        producer = new MapSupport.SyncWriter<T1>(channel.Writer);
        output = new MapSupport.MapAsyncReader<T1, T2>(channel.Reader, selector);
    }
}

/// <summary>An asynchronous push → synchronous pull (composite) map pipe.</summary>
[Flow]
internal sealed partial class MapAsyncProducatorToConsumator<T1, T2>
{
    public MapAsyncProducatorToConsumator(Func<T1, T2> selector, out IAsyncProducator<T1> producer, out IConsumator<T2> output, FlowContext _)
    {
        var channel = Channel.CreateBounded<T1>(2);
        producer = new MapSupport.AsyncWriter<T1>(channel.Writer);
        output = new MapSupport.MapReader<T1, T2>(channel.Reader, selector);
    }
}

/// <summary>An asynchronous push → asynchronous pull (composite) map pipe.</summary>
[Flow]
internal sealed partial class MapAsyncProducatorToAsyncConsumator<T1, T2>
{
    public MapAsyncProducatorToAsyncConsumator(Func<T1, T2> selector, out IAsyncProducator<T1> producer, out IAsyncConsumator<T2> output, FlowContext _)
    {
        var channel = Channel.CreateBounded<T1>(2);
        producer = new MapSupport.AsyncWriter<T1>(channel.Writer);
        output = new MapSupport.MapAsyncReader<T1, T2>(channel.Reader, selector);
    }
}