using Steelax.Pufflow.Operators.Abstractions;
using Steelax.Pufflow.Operators.Aggregators.Warming;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Builders;

/// <summary>
///     An untyped (raw) accumulator of the warming stage configuration, carried through the
///     <c>WarmingBuilder</c> chain. Fields are intentionally <see cref="object" /> because the
///     key and warm types are only introduced by later stages of the chain; the typed projection
///     (<see cref="WarmingBuilderContext{TValue,TKey,TWarm}" />) resolves them at the end.
/// </summary>
/// <typeparam name="TValue">The upstream element type.</typeparam>
internal record WarmingBuilderContext<TValue>(
    Source<IAsyncConsumator<Carrier<TValue>>> Left,
    WarmOptions Options,
    object KeySelector = null!,
    object Policy = null!,
    object JobFactory = null!
);

/// <summary>
///     The resolved projection of a warming builder context, produced from the raw context
///     (<see cref="WarmingBuilderContext{TValue}" />) once the key and warm types are known. This is the
///     single point where the chain's raw fields are downcast to their actual types, so the
///     <see cref="WarmingBuilder{TKey,TValue,TWarm}" /> terminal stage can consume strongly typed values.
/// </summary>
/// <typeparam name="TValue">The upstream element type.</typeparam>
/// <typeparam name="TKey">The key type used to partition the stream for warming.</typeparam>
/// <typeparam name="TWarm">The warming data type produced by an <see cref="IAsyncJob{TKey,TWarm}" />.</typeparam>
internal record WarmingBuilderContext<TValue, TKey, TWarm>(
    Source<IAsyncConsumator<Carrier<TValue>>> Left,
    WarmOptions Options,
    MapSelector<TValue, TKey> KeySelector = null!,
    IWarmPolicy<TKey, TWarm> Policy = null!,
    IJobFactory<TKey, TWarm> JobFactory = null!) where TKey : notnull
{
    /// <summary>
    ///     Resolves a raw builder context into its typed projection by downcasting the erased fields.
    /// </summary>
    /// <param name="context">The untyped context accumulated by the earlier stages of the chain.</param>
    public WarmingBuilderContext(WarmingBuilderContext<TValue> context) : this(
        context.Left,
        context.Options,
        (MapSelector<TValue, TKey>)context.KeySelector,
        (IWarmPolicy<TKey, TWarm>)context.Policy,
        (IJobFactory<TKey, TWarm>)context.JobFactory)
    {}
}

/// <summary>
///     Fluent entry point of the warming stage: selects how the upstream stream is partitioned into
///     warming segments. Start a chain with <see cref="PartitionBy{TKey}" />, continue with
///     <see cref="WarmingBuilder{TValue,TKey}.Prefetch{TWarm}" /> and finish with
///     <see cref="WarmingBuilder{TKey,TValue,TWarm}.Accumulate{TGroup}" />
///     (or its collapsing overload).
/// </summary>
/// <typeparam name="TValue">The upstream element type.</typeparam>
/// <remarks>
///     This is a <c>ref struct</c>: it is intended as a local chain temporary and must not be stored in
///     fields, collections or captured by closures.
/// </remarks>
public readonly ref struct WarmingBuilder<TValue>
{
    private readonly WarmingBuilderContext<TValue> _context;
    
    internal WarmingBuilder(Source<IAsyncConsumator<Carrier<TValue>>> left, WarmOptions options) => _context =
        new WarmingBuilderContext<TValue>(left, options);
    
    /// <summary>
    ///     Selects the key used to partition the stream into warming segments.
    /// </summary>
    /// <typeparam name="TKey">The key type; must be usable as a dictionary key.</typeparam>
    /// <param name="keySelector">Extracts the warming key from each input value.</param>
    /// <returns>The next chain step, carrying the selected key type.</returns>
    public WarmingBuilder<TValue, TKey> PartitionBy<TKey>(MapSelector<TValue, TKey> keySelector) where TKey : notnull =>
        new(_context with { KeySelector = keySelector });
}

/// <summary>
///     Second stage of the warming chain: configures the warming jobs and the policy deciding which keys
///     must be warmed and where their results go.
/// </summary>
/// <typeparam name="TValue">The upstream element type.</typeparam>
/// <typeparam name="TKey">The key type used to partition the stream for warming.</typeparam>
public readonly ref struct WarmingBuilder<TValue, TKey> where TKey : notnull
{
    private readonly WarmingBuilderContext<TValue> _context;
    internal WarmingBuilder(WarmingBuilderContext<TValue> context) => _context = context;

    /// <summary>
    ///     Configures how warming is performed: the factory that creates per-key async jobs and the policy
    ///     that decides which keys are warmed and consumes the warm results.
    /// </summary>
    /// <typeparam name="TWarm">The warming data type produced by the jobs.</typeparam>
    /// <param name="jobFactory">Creates the warming jobs.</param>
    /// <param name="policy">Decides which keys require warming and receives the warm results.</param>
    /// <returns>The terminal chain step, ready to configure grouping.</returns>
    public WarmingBuilder<TKey, TValue, TWarm> Prefetch<TWarm>(IJobFactory<TKey, TWarm> jobFactory, IWarmPolicy<TKey, TWarm> policy) =>
        new(_context with { JobFactory = jobFactory, Policy = policy });
}

/// <summary>
///     Terminal stage of the warming chain: configures the accumulator and builds the
///     <c>WarmProcessor</c>, attaching it to the upstream source.
/// </summary>
/// <typeparam name="TKey">The key type used to partition the stream for warming.</typeparam>
/// <typeparam name="TValue">The upstream element type.</typeparam>
/// <typeparam name="TWarm">The warming data type produced by the jobs.</typeparam>
public readonly ref struct WarmingBuilder<TKey, TValue, TWarm> where TKey : notnull
{
    private readonly WarmingBuilderContext<TValue> _context;
    
    internal WarmingBuilder(WarmingBuilderContext<TValue> context) => _context = context;
    
    /// <summary>
    ///     Finishes the chain with a custom group type: each accumulated group is wrapped together with the
    ///     passthrough value in a <see cref="Unio" /> carrier payload.
    /// </summary>
    /// <typeparam name="TGroup">The type of warmed group results produced by an accumulator.</typeparam>
    /// <param name="accumulatorFactory">Creates the per-key accumulator buffers.</param>
    /// <returns>
    ///     A source emitting <see cref="Carrier{T}" /> items carrying either a passthrough
    ///     <typeparamref name="TValue" /> or an accumulated <typeparamref name="TGroup" />.
    /// </returns>
    public Source<IAsyncProducator<Carrier<Unio<TValue, TGroup>>>> Accumulate<TGroup>(IWarmAccumulatorFactory<TKey, TValue, TGroup> accumulatorFactory)
    {
        var context = new WarmingBuilderContext<TValue, TKey, TWarm>(_context);
        var processor = new WarmProcessor<TKey, TValue, TGroup, TWarm>(
            context.KeySelector,
            context.Options,
            context.Policy,
            context.JobFactory,
            accumulatorFactory);

        return _context.Left.Next(processor.FlowAConsToAProd);
    }
    
    /// <summary>
    ///     Finishes the chain collapsing the group into the same slot as the value. Used when
    ///     <typeparamref name="TValue" /> also serves as the group type, so no union wrapper is needed
    ///     downstream.
    /// </summary>
    /// <param name="accumulatorFactory">Creates the per-key accumulator buffers producing values.</param>
    /// <returns>A source carrying the warmed (value = group) results.</returns>
    public Source<IAsyncProducator<Carrier<TValue>>> Accumulate(IWarmAccumulatorFactory<TKey, TValue> accumulatorFactory)
    {
        var context = new WarmingBuilderContext<TValue, TKey, TWarm>(_context);
        var processor = new WarmProcessor<TKey, TValue, TValue, TWarm>(
            context.KeySelector,
            context.Options,
            context.Policy,
            context.JobFactory,
            accumulatorFactory);
        
        return _context.Left.Next(processor.FlowAConsToAProd).Map(Unwrap);
    }
    
    private static T Unwrap<T>(scoped in Unio<T, T> union) => union.TryPickT0(out var a, out var b) ? a : b;
}