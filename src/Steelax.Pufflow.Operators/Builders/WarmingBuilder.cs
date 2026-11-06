#pragma warning disable CS1591
using Steelax.Pufflow.Operators.Abstractions;
using Steelax.Pufflow.Operators.Aggregators.Warming;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Builders;

/// <summary>An untyped accumulation of the warming stage configuration, carried through the builder chain.</summary>
internal record WarmingBuilderContext<TValue>(
    Fuse<IAsyncConsumator<Carrier<TValue>>> Left,
    WarmOptions Options,
    object KeySelector = null!,
    object Policy = null!,
    object JobFactory = null!
);

internal record WarmingBuilderContext<TValue, TKey, TWarm>(
    Fuse<IAsyncConsumator<Carrier<TValue>>> Left,
    WarmOptions Options,
    MapSelector<TValue, TKey> KeySelector = null!,
    IWarmPolicy<TKey, TWarm> Policy = null!,
    IJobFactory<TKey, TWarm> JobFactory = null!)
{
    public WarmingBuilderContext(WarmingBuilderContext<TValue> context) : this(
        context.Left,
        context.Options,
        (MapSelector<TValue, TKey>)context.KeySelector,
        (IWarmPolicy<TKey, TWarm>)context.Policy,
        (IJobFactory<TKey, TWarm>)context.JobFactory)
    {}
}

public readonly ref struct WarmingBuilder<TValue>
{
    private readonly WarmingBuilderContext<TValue> _context;

    internal WarmingBuilder(Fuse<IAsyncConsumator<Carrier<TValue>>> left, WarmOptions options) => _context =
        new WarmingBuilderContext<TValue>(left, options);

    public WarmingBuilder<TValue, TKey, DefaultHasher<TKey>> PartitionBy<TKey>(MapSelector<TValue, TKey> keySelector) =>
        new(_context with { KeySelector = keySelector });

    public WarmingBuilder<TValue, TKey, THasher> PartitionBy<TKey, THasher>(MapSelector<TValue, TKey> keySelector)
        where THasher : struct, IHashProvider<TKey> =>
        new(_context with { KeySelector = keySelector });
}

public readonly ref struct WarmingBuilder<TValue, TKey, THasher>
    where THasher : struct, IHashProvider<TKey>
{
    private readonly WarmingBuilderContext<TValue> _context;
    internal WarmingBuilder(WarmingBuilderContext<TValue> context) => _context = context;

    public WarmingBuilder<TKey, TValue, TWarm, THasher> Prefetch<TWarm>(
        IJobFactory<TKey, TWarm> jobFactory,
        IWarmPolicy<TKey, TWarm> policy) =>
        new(_context with { JobFactory = jobFactory, Policy = policy });
}

public readonly ref struct WarmingBuilder<TKey, TValue, TWarm, THasher>
    where THasher : struct, IHashProvider<TKey>
{
    private readonly WarmingBuilderContext<TValue> _context;

    internal WarmingBuilder(WarmingBuilderContext<TValue> context) => _context = context;

    public Fuse<IAsyncProducator<Carrier<Unio<TValue, TGroup>>>> Accumulate<TGroup>(
        IWarmAccumulatorFactory<TKey, TValue, TGroup> accumulatorFactory)
    {
        var context = new WarmingBuilderContext<TValue, TKey, TWarm>(_context);

        var pipe = WarmProcessor<TKey, TValue, TGroup, TWarm, THasher>.Configure(
            context.KeySelector,
            context.Options,
            context.Policy,
            context.JobFactory,
            accumulatorFactory);

        return _context.Left.Next(pipe);
    }

    public Fuse<IAsyncProducator<Carrier<TValue>>> Accumulate(
        IWarmAccumulatorFactory<TKey, TValue> accumulatorFactory)
    {
        var context = new WarmingBuilderContext<TValue, TKey, TWarm>(_context);

        var pipe = WarmProcessor<TKey, TValue, TValue, TWarm, THasher>.Configure(
            context.KeySelector,
            context.Options,
            context.Policy,
            context.JobFactory,
            accumulatorFactory);

        return _context.Left.Next(pipe).Map(Unwrap);
    }

    private static T Unwrap<T>(scoped in Unio<T, T> union) => union.TryPickT0(out var a, out var b) ? a : b;
}