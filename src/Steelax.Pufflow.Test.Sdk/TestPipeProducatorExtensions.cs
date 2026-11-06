using JetBrains.Annotations;
using Steelax.Pufflow.Abstractions;
using Steelax.Pufflow.Sdk.Test.PipeProcessors;

namespace Steelax.Pufflow.Sdk.Test;

/// <summary>Map-pipe helpers for push (producator) inputs.</summary>
[PublicAPI]
public static class TestPipeProducatorExtensions
{
    /// <summary>Maps an async push stream to an async push target.</summary>
    [PublicAPI]
    public static Fuse<IAsyncProducator<T2>> ToAsyncProducator<T1, T2>(this Fuse<IAsyncProducator<T1>> left, Func<T1, T2> selector)
        => left.Next(MapAsyncProducatorToAsyncProducator<T1, T2>.Configure(selector));

    /// <summary>Maps an async push stream to a sync push target.</summary>
    [PublicAPI]
    public static Fuse<IProducator<T2>> ToProducator<T1, T2>(this Fuse<IAsyncProducator<T1>> left, Func<T1, T2> selector)
        => left.Next(MapAsyncProducatorToProducator<T1, T2>.Configure(selector));

    /// <summary>Maps an async push stream to an async pull stream.</summary>
    [PublicAPI]
    public static Fuse<IAsyncConsumator<T2>> ToAsyncConsumator<T1, T2>(this Fuse<IAsyncProducator<T1>> left, Func<T1, T2> selector)
        => left.Next(MapAsyncProducatorToAsyncConsumator<T1, T2>.Configure(selector));

    /// <summary>Maps an async push stream to a sync pull stream.</summary>
    [PublicAPI]
    public static Fuse<IConsumator<T2>> ToConsumator<T1, T2>(this Fuse<IAsyncProducator<T1>> left, Func<T1, T2> selector)
        => left.Next(MapAsyncProducatorToConsumator<T1, T2>.Configure(selector));

    /// <summary>Maps a sync push stream to an async push target.</summary>
    [PublicAPI]
    public static Fuse<IAsyncProducator<T2>> ToAsyncProducator<T1, T2>(this Fuse<IProducator<T1>> left, Func<T1, T2> selector)
        => left.Next(MapProducatorToAsyncProducator<T1, T2>.Configure(selector));

    /// <summary>Maps a sync push stream to a sync push target.</summary>
    [PublicAPI]
    public static Fuse<IProducator<T2>> ToProducator<T1, T2>(this Fuse<IProducator<T1>> left, Func<T1, T2> selector)
        => left.Next(MapProducatorToProducator<T1, T2>.Configure(selector));

    /// <summary>Maps a sync push stream to an async pull stream.</summary>
    [PublicAPI]
    public static Fuse<IAsyncConsumator<T2>> ToAsyncConsumator<T1, T2>(this Fuse<IProducator<T1>> left, Func<T1, T2> selector)
        => left.Next(MapProducatorToAsyncConsumator<T1, T2>.Configure(selector));

    /// <summary>Maps a sync push stream to a sync pull stream.</summary>
    [PublicAPI]
    public static Fuse<IConsumator<T2>> ToConsumator<T1, T2>(this Fuse<IProducator<T1>> left, Func<T1, T2> selector)
        => left.Next(MapProducatorToConsumator<T1, T2>.Configure(selector));
}