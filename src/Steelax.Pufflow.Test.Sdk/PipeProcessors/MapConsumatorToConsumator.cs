using System.Threading.Channels;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Sdk.Test.PipeProcessors;

/// <summary>A synchronous pull → synchronous pull map pipe.</summary>
[Flow]
internal sealed partial class MapConsumatorToConsumator<T1, T2>
{
    public MapConsumatorToConsumator(Func<T1, T2> selector, IConsumator<T1> reader, out IConsumator<T2> output, FlowContext context)
    {
        var channel = Channel.CreateBounded<T2>(2);
        output = new MapSupport.SyncReader<T2>(channel.Reader);
        context.RegisterBackgroundProcess(_ => MapSupport.PumpSync(reader, new MapSupport.SyncWriter<T2>(channel.Writer), selector, context.Token));
    }
}

/// <summary>An asynchronous pull → synchronous pull map pipe.</summary>
[Flow]
internal sealed partial class MapAsyncConsumatorToConsumator<T1, T2>
{
    public MapAsyncConsumatorToConsumator(Func<T1, T2> selector, IAsyncConsumator<T1> reader, out IConsumator<T2> output, FlowContext context)
    {
        var channel = Channel.CreateBounded<T2>(2);
        output = new MapSupport.SyncReader<T2>(channel.Reader);
        context.RegisterBackgroundProcess(_ => MapSupport.PumpAsync(reader, new MapSupport.SyncWriter<T2>(channel.Writer), selector, context.Token));
    }
}

/// <summary>A synchronous pull → asynchronous pull map pipe.</summary>
[Flow]
internal sealed partial class MapConsumatorToAsyncConsumator<T1, T2>
{
    public MapConsumatorToAsyncConsumator(Func<T1, T2> selector, IConsumator<T1> reader, out IAsyncConsumator<T2> output, FlowContext context)
    {
        var channel = Channel.CreateBounded<T2>(2);
        output = new MapSupport.AsyncReader<T2>(channel.Reader);
        context.RegisterBackgroundProcess(_ => MapSupport.PumpSyncAsync(reader, new MapSupport.AsyncWriter<T2>(channel.Writer), selector, context.Token));
    }
}

/// <summary>An asynchronous pull → asynchronous pull map pipe.</summary>
[Flow]
internal sealed partial class MapAsyncConsumatorToAsyncConsumator<T1, T2>
{
    public MapAsyncConsumatorToAsyncConsumator(Func<T1, T2> selector, IAsyncConsumator<T1> reader, out IAsyncConsumator<T2> output, FlowContext context)
    {
        var channel = Channel.CreateBounded<T2>(2);
        output = new MapSupport.AsyncReader<T2>(channel.Reader);
        context.RegisterBackgroundProcess(_ => MapSupport.PumpAsyncAsync(reader, new MapSupport.AsyncWriter<T2>(channel.Writer), selector, context.Token));
    }
}