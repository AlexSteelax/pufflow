using Steelax.Pufflow.Operators.Abstractions;
using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Operators.Internal;
using Steelax.Toolkit.HighPerformance.Concurrency.Primitives;

namespace Steelax.Pufflow.Operators.Aggregators.Warming;

/// <summary>
///     [PROTOTYPE] Warms the upstream stream in key segments before forwarding values downstream.
/// </summary>
/// <typeparam name="TKey">The key type used to partition the stream for warming.</typeparam>
/// <typeparam name="TValue">The type of input values.</typeparam>
/// <typeparam name="TGroup">The type of warmed group results produced by an accumulator.</typeparam>
/// <typeparam name="TWarm">The warming data type produced by an <see cref="IAsyncJob{TKey,TWarm}" />.</typeparam>
/// <remarks>
///     Consumes <see cref="Carrier{T}" /> items of <typeparamref name="TValue" />: an element with data is a value
///     to process; an empty element (<see cref="Carrier{T}.HasValue" /> is <see langword="false" />) is a pure
///     progress point (no data). It emits <see cref="Carrier{T}" /> items whose payload is
///     <see cref="Unio{T,TGroup}" />: the carried value is either a passthrough <typeparamref name="TValue" /> or an
///     accumulated <typeparamref name="TGroup" /> result. An empty carrier (no payload) carries the commit/progress
///     watermark only.
///     <para />
///     The processor combines two mechanisms inherited from the old Kafka warm source:
///     <list type="bullet">
///         <item>
///             <description>
///                 a per-key delayed buffer (the <c>FlowGate</c> dictionary): values of warmable
///                 keys are held back in their <see cref="WarmAccumulator{TValue,TGroup}" /> until the warm result is
///                 ready, so they are not emitted out of order;
///             </description>
///         </item>
///         <item>
///             <description>
///                 the <see cref="Warmer{TKey,TWarm}" />: key segmentation, bounded concurrent warm
///                 jobs, and head-of-line (watermark-ordered) emission — it is the watermark barrier.
///             </description>
///         </item>
///     </list>
///     <para />
///     The warm result for a key is delivered to the <see cref="IWarmPolicy{TKey,TWarm}" /> (which decides
///     whether a key needs warming and can cache the result). The processor drains warmed segments into
///     the output; a segment that cannot be fully drained (output is full) is retained and finished before
///     a fresh segment is extracted from the warmer (see the draining partial).
/// </remarks>
[Flow]
internal sealed partial class WarmProcessor<TKey, TValue, TGroup, TWarm> : IAsyncDisposable
    where TKey : notnull
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="WarmProcessor{TKey,TValue,TGroup,TWarm}" /> class.
    /// </summary>
    /// <param name="options">
    ///     Tuning values for the warmer (concurrency, queue, segment size) and the processor (delayed-queue
    ///     weight budget, watchdog period, and the idle linger interval that seals partially filled tails).
    /// </param>
    /// <param name="jobFactory">Creates the warming jobs; owned by the warmer created here.</param>
    /// <param name="keySelector">Selects the warming key for each input value.</param>
    /// <param name="policy">Decides which keys require warming and receives the warm result.</param>
    /// <param name="accumulatorFactory">Creates the per-key accumulator buffers.</param>
    /// <exception cref="ArgumentNullException">
    ///     Thrown when any of <paramref name="jobFactory" />, <paramref name="keySelector" />,
    ///     <paramref name="policy" /> or <paramref name="accumulatorFactory" /> is <see langword="null" />.
    /// </exception>
    public WarmProcessor(
        MapSelector<TValue, TKey> keySelector,
        WarmOptions options,
        IWarmPolicy<TKey, TWarm> policy,
        IJobFactory<TKey, TWarm> jobFactory,
        IWarmAccumulatorFactory<TKey, TValue, TGroup> accumulatorFactory)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(jobFactory);
        ArgumentNullException.ThrowIfNull(keySelector);
        ArgumentNullException.ThrowIfNull(policy);
        ArgumentNullException.ThrowIfNull(accumulatorFactory);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.QueueWeightLimit);
        ArgumentOutOfRangeException.ThrowIfZero(options.WatchdogPeriod.TotalMilliseconds, nameof(options.WatchdogPeriod));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.SegmentTtl.TotalMilliseconds, nameof(options.SegmentTtl));

        _warmer = new Warmer<TKey, TWarm>(options.MaxConcurrency, options.MaxQueued, options.SegmentCapacity, jobFactory);
        
        _keySelector = keySelector;
        _policy = policy;
        _accumulatorFactory = accumulatorFactory;
        _queueWeightLimit = options.QueueWeightLimit;

        _fanIn = new FanInSlim();
        _delayedQueue = new Dictionary<TKey, WarmAccumulator<TValue, TGroup>>();

        // Wake the loop when a warm job completes. The input/output readiness is wired in Fuse once the
        // wrapping ManualConsumator / ManualProducator are assigned.
        _warmer.OnReady += _fanIn.GetSignalCallback(WarmSlot).Handler;

        _segmentTtl = options.SegmentTtl;

        // The watchdog periodically wakes the loop to re-check the state. It is created once (always valid);
        // the consumer loop calls Start() to begin the schedule, which is inert when the period is disabled
        // (InfiniteTimeSpan), and stops it on exit.
        _watchdog = new WatchTimer<FanInSlim>(static state => state.Signal(WatchdogSlot), _fanIn, options.WatchdogPeriod);

        // The one-shot linger timer signals LingerSlot after the idle interval so a partial tail is sealed.
        // It is created once here and armed directly (_linger.Start(_segmentLinger)) when the source is silent.
        _ttl = new WatchTimer<FanInSlim>(static state => state.Signal(LingerSlot), _fanIn);
    }

    /// <summary>
    ///     Fuses the processor into the flow: registers the main consumer loop as a background task that
    ///     reads <paramref name="source" />, warms it in key segments and writes the result into
    ///     <paramref name="output" />.
    /// </summary>
    /// <param name="source">The upstream source to consume.</param>
    /// <param name="output">The downstream producer to write passthrough/group results into.</param>
    /// <param name="context">The flow context that owns the background task and the cancellation token.</param>
    public void Fuse(IAsyncConsumator<Carrier<TValue>> source, IAsyncProducator<Carrier<Unio<TValue, TGroup>>> output, FlowContext context)
    {
        // Store the wrapped source and the wrapped output producer for the consumer loop.
        _source = ManualConsumator<Carrier<TValue>>.Create(source);
        _writer = ManualProducator<Carrier<Unio<TValue, TGroup>>>.Create(output);

        // Wake the loop when the source produces a value (or completes) or the output frees capacity.
        _source.OnReady += _fanIn.GetSignalCallback(InputSlot).Handler;
        _writer.OnReady += _fanIn.GetSignalCallback(OutputSlot).Handler;

        // Register this processor for release (watchdog/warmer) when the pipeline completes.
        context.RegisterDisposable(this);
        context.RegisterBackground(() => InternalExecuteAsync(context));
    }

    /// <summary>
    ///     Releases the resources owned by this processor: stops the watchdog and disposes the warmer
    ///     (which cancels and awaits its in-flight warm jobs). Called through <see cref="Fuse" /> when the
    ///     pipeline completes (normal or canceled).
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        await _watchdog.DisposeAsync();
        await _ttl.DisposeAsync();
        await _warmer.DisposeAsync();
    }
}