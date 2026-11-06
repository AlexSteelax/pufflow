using System.Diagnostics;
using System.Threading.Channels;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Sdk.Test.SourceProcessors;

/// <summary>
///     An asynchronous push source over a <see cref="Channel{T}" />: reads the channel and pushes every
///     element into the supplied asynchronous target, honouring backpressure.
/// </summary>
/// <typeparam name="T">The element type.</typeparam>
[Flow]
internal sealed partial class FlowAsyncProducatorSource<T>
{
    /// <summary>Initializes the source and registers the push production loop.</summary>
    /// <param name="channel">The channel read by the source.</param>
    /// <param name="writer">The asynchronous push target to write into.</param>
    /// <param name="context">The flow context for cancellation and background work.</param>
    public FlowAsyncProducatorSource(Channel<T> channel, IAsyncProducator<T> writer, FlowContext context)
    {
        context.RegisterBackgroundProcess(_ => ProduceLoopAsync(channel.Reader, writer, context));
    }

    private static async Task ProduceLoopAsync(ChannelReader<T> reader, IAsyncProducator<T> output, CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                if (reader.TryPeek(out var item))
                {
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        if (output.TryWrite(item))
                        {
                            _ = reader.TryRead(out _);
                            break;
                        }

                        await output.WaitToWriteAsync();
                    }

                    continue;
                }

                if (!await reader.WaitToReadAsync(cancellationToken))
                    break;
            }
        }
        catch (Exception ex)
        {
            Trace.WriteLine($"[FlowAsyncProducatorSource] fault: {ex}");
            output.TryComplete(ex);
        }
        finally
        {
            output.TryComplete();
        }
    }
}