using System.Threading.Channels;
using JetBrains.Annotations;
using Steelax.Pufflow.Abstractions;
using Steelax.Pufflow.Sdk.Test.SinkProcessors;

namespace Steelax.Pufflow.Sdk.Test;

/// <summary>Sink helpers for tests: terminates a chain into a channel or a discard sink.</summary>
[PublicAPI]
public static class TestSinkExtensions
{
    private static Channel<T> CreateChannel<T>() => Channel.CreateUnbounded<T>(new UnboundedChannelOptions
    {
        SingleReader = false,
        SingleWriter = true,
        AllowSynchronousContinuations = true
    });

    /// <summary>Terminates an async pull chain into a channel, returning its reader.</summary>
    [PublicAPI]
    public static void Consume<T>(this Fuse<IAsyncConsumator<T>> left, out ChannelReader<T> reader)
    {
        var channel = CreateChannel<T>();
        reader = channel.Reader;
        left.End(FlowAsyncConsumatorSink<T>.Configure(channel));
    }

    /// <summary>Terminates an async pull chain into a discard sink.</summary>
    [PublicAPI]
    public static void Consume<T>(this Fuse<IAsyncConsumator<T>> left)
        => left.End(FlowNullAsyncConsumatorSink<T>.Configure());

    /// <summary>Terminates a sync pull chain into a channel, returning its reader.</summary>
    [PublicAPI]
    public static void Consume<T>(this Fuse<IConsumator<T>> left, out ChannelReader<T> reader)
    {
        var channel = CreateChannel<T>();
        reader = channel.Reader;
        left.End(FlowConsumatorSink<T>.Configure(channel));
    }

    /// <summary>Terminates a sync pull chain into a discard sink.</summary>
    [PublicAPI]
    public static void Consume<T>(this Fuse<IConsumator<T>> left)
        => left.End(FlowNullConsumatorSink<T>.Configure());

    /// <summary>Terminates an async push chain into a channel, returning its reader.</summary>
    [PublicAPI]
    public static void Consume<T>(this Fuse<IAsyncProducator<T>> left, out ChannelReader<T> reader)
    {
        var channel = CreateChannel<T>();
        reader = channel.Reader;
        left.End(FlowAsyncProducatorSink<T>.Configure(channel));
    }

    /// <summary>Terminates an async push chain into a discard sink.</summary>
    [PublicAPI]
    public static void Consume<T>(this Fuse<IAsyncProducator<T>> left)
        => left.End(FlowNullAsyncProducatorSink<T>.Configure());

    /// <summary>Terminates a sync push chain into a channel, returning its reader.</summary>
    [PublicAPI]
    public static void Consume<T>(this Fuse<IProducator<T>> left, out ChannelReader<T> reader)
    {
        var channel = CreateChannel<T>();
        reader = channel.Reader;
        left.End(FlowProducatorSink<T>.Configure(channel));
    }

    /// <summary>Terminates a sync push chain into a discard sink.</summary>
    [PublicAPI]
    public static void Consume<T>(this Fuse<IProducator<T>> left)
        => left.End(FlowNullProducatorSink<T>.Configure());
}