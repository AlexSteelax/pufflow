using System.Collections.Generic;
using System.Threading.Channels;
using JetBrains.Annotations;
using Steelax.Pufflow.Abstractions;
using Steelax.Pufflow.Sdk.Test.Transitions;

namespace Steelax.Pufflow.Sdk.Test;

/// <summary>Enables building pipelines around the standard .NET asynchronous-enumerator model.</summary>
[PublicAPI]
public static class TestEnumeratorExtensions
{
    private static Channel<T> CreateChannel<T>() => Channel.CreateUnbounded<T>(new UnboundedChannelOptions
    {
        SingleReader = true,
        SingleWriter = false,
        AllowSynchronousContinuations = true
    });

    private static void InitialFill<T>(ChannelWriter<T> writer, IEnumerable<T>? initial)
    {
        if (initial is null)
            return;

        foreach (var value in initial)
            writer.TryWrite(value);
    }

    /// <summary>Opens an asynchronous-enumerator pull source, returning the writer to feed it.</summary>
    [PublicAPI]
    public static Fuse<IAsyncEnumerator<T>> OnEnumeratorSource<T>(this FlowSource flowSource, out ChannelWriter<T> writer, IEnumerable<T>? initial = null)
    {
        var channel = CreateChannel<T>();
        InitialFill(channel.Writer, initial);
        writer = channel.Writer;
        return flowSource.On(FlowEnumeratorSource<T>.Configure(channel));
    }

    /// <summary>Projects an asynchronous-enumerator stream.</summary>
    [PublicAPI]
    public static Fuse<IAsyncEnumerator<T2>> ToAsyncEnumerator<T1, T2>(this Fuse<IAsyncEnumerator<T1>> left, Func<T1, T2> selector)
        => left.Next(MapAsyncEnumerator<T1, T2>.Configure(selector));

    /// <summary>Bridges an enumerator stream into an asynchronous consumator pipe (via the adapter).</summary>
    [PublicAPI]
    public static Fuse<IAsyncConsumator<T2>> ToAsyncConsumator<T1, T2>(this Fuse<IAsyncEnumerator<T1>> left, Func<T1, T2> selector)
        => left.Next(PipeProcessors.MapAsyncConsumatorToAsyncConsumator<T1, T2>.Configure(selector));

    /// <summary>Bridges an asynchronous consumator stream into an enumerator pipe (via the adapter).</summary>
    [PublicAPI]
    public static Fuse<IAsyncEnumerator<T2>> ToAsyncEnumerator<T1, T2>(this Fuse<IAsyncConsumator<T1>> left, Func<T1, T2> selector)
        => left.Next(MapAsyncEnumerator<T1, T2>.Configure(selector));

    /// <summary>Terminates an asynchronous-enumerator chain into a channel, returning its reader.</summary>
    [PublicAPI]
    public static void Consume<T>(this Fuse<IAsyncEnumerator<T>> left, out ChannelReader<T> reader)
    {
        var channel = CreateChannel<T>();
        reader = channel.Reader;
        left.End(FlowEnumeratorSink<T>.Configure(channel));
    }
}