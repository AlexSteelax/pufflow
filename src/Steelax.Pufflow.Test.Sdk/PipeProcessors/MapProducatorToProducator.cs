using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Sdk.Test.PipeProcessors;

/// <summary>A synchronous push → synchronous push map pipe.</summary>
[Flow]
internal sealed partial class MapProducatorToProducator<T1, T2>
{
    public MapProducatorToProducator(Func<T1, T2> selector, out IProducator<T1> producer, IProducator<T2> writer, FlowContext _)
    {
        producer = new MapSupport.MapProducator<T1, T2>(writer, selector);
    }
}

/// <summary>A synchronous push → asynchronous push map pipe.</summary>
[Flow]
internal sealed partial class MapProducatorToAsyncProducator<T1, T2>
{
    public MapProducatorToAsyncProducator(Func<T1, T2> selector, out IProducator<T1> producer, IAsyncProducator<T2> writer, FlowContext _)
    {
        producer = new MapSupport.MapProducator<T1, T2>(writer, selector);
    }
}

/// <summary>An asynchronous push → synchronous push map pipe.</summary>
[Flow]
internal sealed partial class MapAsyncProducatorToProducator<T1, T2>
{
    public MapAsyncProducatorToProducator(Func<T1, T2> selector, out IAsyncProducator<T1> producer, IProducator<T2> writer, FlowContext _)
    {
        producer = new MapSupport.MapAsyncProducator<T1, T2>(writer, selector);
    }
}

/// <summary>An asynchronous push → asynchronous push map pipe.</summary>
[Flow]
internal sealed partial class MapAsyncProducatorToAsyncProducator<T1, T2>
{
    public MapAsyncProducatorToAsyncProducator(Func<T1, T2> selector, out IAsyncProducator<T1> producer, IAsyncProducator<T2> writer, FlowContext _)
    {
        producer = new MapSupport.MapAsyncProducator<T1, T2>(writer, selector);
    }
}