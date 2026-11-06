using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Sdk.Test.PipeProcessors;

/// <summary>A synchronous pull → synchronous push (hybrid) map pipe.</summary>
[Flow]
internal sealed partial class MapConsumatorToProducator<T1, T2>
{
    public MapConsumatorToProducator(Func<T1, T2> selector, IConsumator<T1> reader, IProducator<T2> writer, FlowContext context)
    {
        context.RegisterBackgroundProcess(_ => MapSupport.PumpSync(reader, writer, selector, context.Token));
    }
}

/// <summary>An asynchronous pull → synchronous push (hybrid) map pipe.</summary>
[Flow]
internal sealed partial class MapAsyncConsumatorToProducator<T1, T2>
{
    public MapAsyncConsumatorToProducator(Func<T1, T2> selector, IAsyncConsumator<T1> reader, IProducator<T2> writer, FlowContext context)
    {
        context.RegisterBackgroundProcess(_ => MapSupport.PumpAsync(reader, writer, selector, context.Token));
    }
}

/// <summary>A synchronous pull → asynchronous push (hybrid) map pipe.</summary>
[Flow]
internal sealed partial class MapConsumatorToAsyncProducator<T1, T2>
{
    public MapConsumatorToAsyncProducator(Func<T1, T2> selector, IConsumator<T1> reader, IAsyncProducator<T2> writer, FlowContext context)
    {
        context.RegisterBackgroundProcess(_ => MapSupport.PumpSyncAsync(reader, writer, selector, context.Token));
    }
}

/// <summary>An asynchronous pull → asynchronous push (hybrid) map pipe.</summary>
[Flow]
internal sealed partial class MapAsyncConsumatorToAsyncProducator<T1, T2>
{
    public MapAsyncConsumatorToAsyncProducator(Func<T1, T2> selector, IAsyncConsumator<T1> reader, IAsyncProducator<T2> writer, FlowContext context)
    {
        context.RegisterBackgroundProcess(_ => MapSupport.PumpAsyncAsync(reader, writer, selector, context.Token));
    }
}