using System.Threading.Channels;
using JetBrains.Annotations;
using Steelax.Pufflow.Abstractions;
using Steelax.Pufflow.Sdk.Test.SourceProcessors;

namespace Steelax.Pufflow.Sdk.Test;

/// <summary>Source-opening helpers for tests: seeds a channel and opens a flow over it.</summary>
[PublicAPI]
public static class TestSourceExtensions
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

        foreach (var item in initial)
            if (!writer.TryWrite(item))
                throw new InvalidOperationException();
    }

    /// <summary>Opens an asynchronous push source, returning the writer to feed it.</summary>
    [PublicAPI]
    public static Fuse<IAsyncProducator<T>> OnAsyncProducatorSource<T>(this FlowSource flowSource, out ChannelWriter<T> writer, IEnumerable<T>? initial = null)
    {
        var channel = CreateChannel<T>();
        InitialFill(channel.Writer, initial);
        writer = channel.Writer;
        return flowSource.On(FlowAsyncProducatorSource<T>.Configure(channel));
    }

    /// <summary>Opens a synchronous push source, returning the writer to feed it.</summary>
    [PublicAPI]
    public static Fuse<IProducator<T>> OnProducatorSource<T>(this FlowSource flowSource, out ChannelWriter<T> writer, IEnumerable<T>? initial = null)
    {
        var channel = CreateChannel<T>();
        InitialFill(channel.Writer, initial);
        writer = channel.Writer;
        return flowSource.On(FlowProducatorSource<T>.Configure(channel));
    }

    /// <summary>Opens an asynchronous pull source, returning the writer to feed it.</summary>
    [PublicAPI]
    public static Fuse<IAsyncConsumator<T>> OnAsyncConsumatorSource<T>(this FlowSource flowSource, out ChannelWriter<T> writer, IEnumerable<T>? initial = null)
    {
        var channel = CreateChannel<T>();
        InitialFill(channel.Writer, initial);
        writer = channel.Writer;
        return flowSource.On(FlowAsyncConsumatorSource<T>.Configure(channel));
    }

    /// <summary>Opens a synchronous pull source, returning the writer to feed it.</summary>
    [PublicAPI]
    public static Fuse<IConsumator<T>> OnConsumatorSource<T>(this FlowSource flowSource, out ChannelWriter<T> writer, IEnumerable<T>? initial = null)
    {
        var channel = CreateChannel<T>();
        InitialFill(channel.Writer, initial);
        writer = channel.Writer;
        return flowSource.On(FlowConsumatorSource<T>.Configure(channel));
    }

    /// <summary>Opens an asynchronous push source pre-filled with the given items.</summary>
    [PublicAPI]
    public static Fuse<IAsyncProducator<T>> OnAsyncProducatorSource<T>(this FlowSource flowSource, IEnumerable<T> items)
    {
        var fuse = flowSource.OnAsyncProducatorSource<T>(out var writer, items);
        writer.TryComplete();
        return fuse;
    }

    /// <summary>Opens a synchronous push source pre-filled with the given items.</summary>
    [PublicAPI]
    public static Fuse<IProducator<T>> OnProducatorSource<T>(this FlowSource flowSource, IEnumerable<T> items)
    {
        var fuse = flowSource.OnProducatorSource<T>(out var writer, items);
        writer.TryComplete();
        return fuse;
    }

    /// <summary>Opens an asynchronous pull source pre-filled with the given items.</summary>
    [PublicAPI]
    public static Fuse<IAsyncConsumator<T>> OnAsyncConsumatorSource<T>(this FlowSource flowSource, IEnumerable<T> items)
    {
        var fuse = flowSource.OnAsyncConsumatorSource<T>(out var writer, items);
        writer.TryComplete();
        return fuse;
    }

    /// <summary>Opens a synchronous pull source pre-filled with the given items.</summary>
    [PublicAPI]
    public static Fuse<IConsumator<T>> OnConsumatorSource<T>(this FlowSource flowSource, IEnumerable<T> items)
    {
        var fuse = flowSource.OnConsumatorSource<T>(out var writer, items);
        writer.TryComplete();
        return fuse;
    }
}