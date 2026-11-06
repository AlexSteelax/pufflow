using System.Diagnostics;
using System.Threading.Channels;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Sdk.Test.SourceProcessors;

/// <summary>
///     A synchronous push source over a <see cref="Channel{T}" />: reads the channel and pushes every
///     element into the supplied synchronous target.
/// </summary>
/// <typeparam name="T">The element type.</typeparam>
[Flow]
internal sealed partial class FlowProducatorSource<T>
{
    /// <summary>Initializes the source and registers the push production loop.</summary>
    /// <param name="channel">The channel read by the source.</param>
    /// <param name="writer">The synchronous push target to write into.</param>
    /// <param name="context">The flow context for cancellation and background work.</param>
    public FlowProducatorSource(Channel<T> channel, IProducator<T> writer, FlowContext context)
    {
        context.RegisterBackgroundProcess(_ => ProduceLoopAsync(channel.Reader, writer, context));
    }

    private static async Task ProduceLoopAsync(ChannelReader<T> reader, IProducator<T> output, CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                if (!reader.TryPeek(out var item))
                {
                    if (!await reader.WaitToReadAsync(cancellationToken))
                        break;

                    continue;
                }

                while (!cancellationToken.IsCancellationRequested)
                {
                    if (!output.TryWrite(item))
                    {
                        await Task.Yield();
                        continue;
                    }

                    _ = reader.TryRead(out _);
                    break;
                }
            }
        }
        catch (Exception ex)
        {
            Trace.WriteLine($"[FlowProducatorSource] fault: {ex}");
            output.TryComplete(ex);
        }
        finally
        {
            output.TryComplete();
        }
    }
}