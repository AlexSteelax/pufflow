using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Sdk.Test.SinkProcessors;

/// <summary>
///     A synchronous pull sink that drains the upstream stream, discarding every value.
/// </summary>
/// <typeparam name="T">The element type.</typeparam>
[Flow]
internal sealed partial class FlowNullConsumatorSink<T>
{
    /// <summary>Initializes the sink and registers the drain loop.</summary>
    /// <param name="reader">The upstream synchronous pull stream.</param>
    /// <param name="context">The flow context for cancellation and background work.</param>
    public FlowNullConsumatorSink(IConsumator<T> reader, FlowContext context)
    {
        context.RegisterBackgroundProcess(_ => ConsumeLoopAsync(reader, context));
    }

    private static async Task ConsumeLoopAsync(IConsumator<T> reader, FlowContext context)
    {
        var token = context.Token;

        while (!token.IsCancellationRequested)
        {
            if (reader.TryRead(out _))
                continue;

            if (reader.IsCompleted)
                break;

            await Task.Yield();
        }
    }
}