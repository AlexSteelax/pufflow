using System.Threading.Channels;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Sdk.Test.SinkProcessors;

/// <summary>
///     A synchronous pull sink over a <see cref="Channel{T}" />: drains the upstream synchronous reader
///     into the channel.
/// </summary>
/// <typeparam name="T">The element type.</typeparam>
[Flow]
internal sealed partial class FlowConsumatorSink<T>
{
    /// <summary>Initializes the sink and registers the drain loop.</summary>
    /// <param name="channel">The channel written by the sink.</param>
    /// <param name="reader">The upstream synchronous pull stream.</param>
    /// <param name="context">The flow context for cancellation and background work.</param>
    public FlowConsumatorSink(Channel<T> channel, IConsumator<T> reader, FlowContext context)
    {
        context.RegisterBackgroundProcess(_ => ConsumeLoopAsync(reader, channel.Writer, context));
    }

    private static async Task ConsumeLoopAsync(IConsumator<T> reader, ChannelWriter<T> writer, CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                if (reader.TryRead(out var item))
                {
                    await writer.WriteAsync(item, cancellationToken);
                    continue;
                }

                if (reader.IsCompleted)
                    break;

                await Task.Yield();
            }
        }
        catch (Exception ex)
        {
            writer.TryComplete(ex);
        }
        finally
        {
            writer.TryComplete();
        }
    }
}