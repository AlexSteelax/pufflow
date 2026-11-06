using System.Threading.Channels;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Sdk.Test.SinkProcessors;

/// <summary>
///     An asynchronous pull sink over a <see cref="Channel{T}" />: drains the upstream asynchronous reader
///     into the channel.
/// </summary>
/// <typeparam name="T">The element type.</typeparam>
[Flow]
internal sealed partial class FlowAsyncConsumatorSink<T>
{
    /// <summary>Initializes the sink and registers the drain loop.</summary>
    /// <param name="channel">The channel written by the sink.</param>
    /// <param name="reader">The upstream asynchronous pull stream.</param>
    /// <param name="context">The flow context for cancellation and background work.</param>
    public FlowAsyncConsumatorSink(Channel<T> channel, IAsyncConsumator<T> reader, FlowContext context)
    {
        context.RegisterBackgroundProcess(_ => ConsumeLoopAsync(reader, channel.Writer, context));
    }

    private static async Task ConsumeLoopAsync(IAsyncConsumator<T> reader, ChannelWriter<T> writer, CancellationToken cancellationToken)
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

                if (!await reader.WaitToReadAsync())
                    break;
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