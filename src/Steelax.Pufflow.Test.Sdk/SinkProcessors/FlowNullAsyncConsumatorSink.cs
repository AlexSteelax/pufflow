using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Sdk.Test.SinkProcessors;

/// <summary>
///     An asynchronous pull sink that drains the upstream stream, discarding every value.
/// </summary>
/// <typeparam name="T">The element type.</typeparam>
[Flow]
internal sealed partial class FlowNullAsyncConsumatorSink<T>
{
    /// <summary>Initializes the sink and registers the drain loop.</summary>
    /// <param name="reader">The upstream asynchronous pull stream.</param>
    /// <param name="context">The flow context for cancellation and background work.</param>
    public FlowNullAsyncConsumatorSink(IAsyncConsumator<T> reader, FlowContext context)
    {
        context.RegisterBackgroundProcess(_ => ConsumeLoopAsync(reader, context));
    }

    private static async Task ConsumeLoopAsync(IAsyncConsumator<T> reader, FlowContext context)
    {
        var token = context.Token;

        while (!token.IsCancellationRequested)
        {
            if (reader.TryRead(out _))
                continue;

            if (!await reader.WaitToReadAsync())
                break;
        }
    }
}