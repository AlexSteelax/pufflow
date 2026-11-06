using System.Threading.Channels;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Sdk.Test.Transitions;

/// <summary>A pull source exposing a channel's contents as a standard .NET <see cref="IAsyncEnumerator{T}" />.</summary>
[Flow]
internal sealed partial class FlowEnumeratorSource<T>
{
    public FlowEnumeratorSource(Channel<T> channel, out IAsyncEnumerator<T> output, FlowContext _)
    {
        output = channel.Reader.ReadAllAsync().GetAsyncEnumerator();
    }
}

/// <summary>A 1:1 pipe that projects an asynchronous enumerator stream.</summary>
[Flow]
internal sealed partial class MapAsyncEnumerator<T1, T2>
{
    public MapAsyncEnumerator(Func<T1, T2> selector, IAsyncEnumerator<T1> source, out IAsyncEnumerator<T2> target, FlowContext _)
    {
        ArgumentNullException.ThrowIfNull(selector);
        target = MapCore(source, selector);
    }

    private static async IAsyncEnumerator<T2> MapCore(IAsyncEnumerator<T1> source, Func<T1, T2> selector)
    {
        try
        {
            while (await source.MoveNextAsync())
                yield return selector(source.Current);
        }
        finally
        {
            await source.DisposeAsync();
        }
    }
}

/// <summary>A pull sink that drains an asynchronous enumerator stream into a channel.</summary>
[Flow]
internal sealed partial class FlowEnumeratorSink<T>
{
    public FlowEnumeratorSink(Channel<T> channel, IAsyncEnumerator<T> source, FlowContext context)
    {
        context.RegisterBackgroundProcess(_ => DrainAsync(source, channel.Writer, context));
    }

    private static async Task DrainAsync(IAsyncEnumerator<T> source, ChannelWriter<T> writer, CancellationToken token)
    {
        try
        {
            while (await source.MoveNextAsync())
                await writer.WriteAsync(source.Current, token);
        }
        finally
        {
            writer.TryComplete();
            await source.DisposeAsync();
        }
    }
}