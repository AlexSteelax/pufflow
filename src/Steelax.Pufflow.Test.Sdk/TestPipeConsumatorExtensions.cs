using JetBrains.Annotations;
using Steelax.Pufflow.Abstractions;
using Steelax.Pufflow.Sdk.Test.PipeProcessors;

namespace Steelax.Pufflow.Sdk.Test;

/// <summary>Map-pipe helpers for pull (consumator) inputs.</summary>
[PublicAPI]
public static class TestPipeConsumatorExtensions
{
    /// <summary>Maps an async pull stream to an async push target.</summary>
    [PublicAPI]
    public static Fuse<IAsyncProducator<T2>> ToAsyncProducator<T1, T2>(this Fuse<IAsyncConsumator<T1>> left, Func<T1, T2> selector)
        => left.Next(MapAsyncConsumatorToAsyncProducator<T1, T2>.Configure(selector));

    /// <summary>Maps an async pull stream to a sync push target.</summary>
    [PublicAPI]
    public static Fuse<IProducator<T2>> ToProducator<T1, T2>(this Fuse<IAsyncConsumator<T1>> left, Func<T1, T2> selector)
        => left.Next(MapAsyncConsumatorToProducator<T1, T2>.Configure(selector));

    /// <summary>Maps an async pull stream to an async pull stream.</summary>
    [PublicAPI]
    public static Fuse<IAsyncConsumator<T2>> ToAsyncConsumator<T1, T2>(this Fuse<IAsyncConsumator<T1>> left, Func<T1, T2> selector)
        => left.Next(MapAsyncConsumatorToAsyncConsumator<T1, T2>.Configure(selector));

    /// <summary>Maps an async pull stream to a sync pull stream.</summary>
    [PublicAPI]
    public static Fuse<IConsumator<T2>> ToConsumator<T1, T2>(this Fuse<IAsyncConsumator<T1>> left, Func<T1, T2> selector)
        => left.Next(MapAsyncConsumatorToConsumator<T1, T2>.Configure(selector));

    /// <summary>Maps a sync pull stream to an async push target.</summary>
    [PublicAPI]
    public static Fuse<IAsyncProducator<T2>> ToAsyncProducator<T1, T2>(this Fuse<IConsumator<T1>> left, Func<T1, T2> selector)
        => left.Next(MapConsumatorToAsyncProducator<T1, T2>.Configure(selector));

    /// <summary>Maps a sync pull stream to a sync push target.</summary>
    [PublicAPI]
    public static Fuse<IProducator<T2>> ToProducator<T1, T2>(this Fuse<IConsumator<T1>> left, Func<T1, T2> selector)
        => left.Next(MapConsumatorToProducator<T1, T2>.Configure(selector));

    /// <summary>Maps a sync pull stream to an async pull stream.</summary>
    [PublicAPI]
    public static Fuse<IAsyncConsumator<T2>> ToAsyncConsumator<T1, T2>(this Fuse<IConsumator<T1>> left, Func<T1, T2> selector)
        => left.Next(MapConsumatorToAsyncConsumator<T1, T2>.Configure(selector));

    /// <summary>Maps a sync pull stream to a sync pull stream.</summary>
    [PublicAPI]
    public static Fuse<IConsumator<T2>> ToConsumator<T1, T2>(this Fuse<IConsumator<T1>> left, Func<T1, T2> selector)
        => left.Next(MapConsumatorToConsumator<T1, T2>.Configure(selector));
}