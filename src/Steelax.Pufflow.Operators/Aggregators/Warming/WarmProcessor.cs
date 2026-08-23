using Steelax.Pufflow.Operators.Abstractions;
using Steelax.Pufflow.Operators.Common;
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
///     Consumes <see cref="Watermarked{T}" /> messages and emits <see cref="Unio{T,TGroup,Watermark}" />
///     items downstream: <c>T0</c> — a passthrough value, <c>T1</c> — an accumulated group result,
///     <c>T2</c> — a watermark marker emitted after the data it covers (a commit/progress point).
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
internal sealed partial class WarmProcessor<TKey, TValue, TGroup, TWarm>
    where TKey : notnull
{
    /// <summary>Observes the input <c>WaitToReadAsync</c> and signals the loop when the source becomes ready.</summary>
    private readonly EventTask<bool> _input;

    /// <summary>Observes the output <c>WaitToWriteAsync</c> and signals the loop when capacity frees up.</summary>
    private readonly EventTask<bool> _output;

    /// <summary>The fan-in slot signaled by the <see cref="Warmer{TKey,TWarm}" /> when work completes.</summary>
    private const int WarmSlot = 1;

    /// <summary>The fan-in slot signaled when the output producer frees capacity (<see cref="_output" />).</summary>
    private const int OutputSlot = 2;

    /// <summary>The fan-in slot signaled when the input consumator has data (<see cref="_input" />).</summary>
    private const int InputSlot = 3;

    /// <summary>The fan-in slot used only to wake a sleeping loop on cancellation (the source bridge uses slot 31).</summary>
    private const int CancellationSlot = 0;

    /// <summary>The fan-in slot signaled periodically by the watchdog timer to re-check a sleeping loop.</summary>
    private const int WatchdogSlot = 4;

    /// <summary>Creates the per-key accumulator buffers.</summary>
    private readonly IWarmAccumulatorFactory<TKey, TValue, TGroup> _accumulatorFactory;

    /// <summary>The per-key delayed buffers (the FlowGate dictionary).</summary>
    private readonly Dictionary<TKey, WarmAccumulator<TValue, TGroup>> _delayedQueue;

    /// <summary>The shared fan-in multiplexing source readiness and warm completion.</summary>
    private readonly FanInSlim _fanIn;

    /// <summary>Selects the warming key for a value.</summary>
    private readonly MapSelector<TValue, TKey> _keySelector;

    /// <summary>Decides whether a key requires warming and receives the warm result for a key.</summary>
    private readonly IWarmPolicy<TKey, TWarm> _policy;

    /// <summary>The maximum total weight the per-key delayed buffers may hold (the buffer budget limit).</summary>
    private readonly long _queueWeightLimit;

    /// <summary>The warmer providing key segmentation, concurrent warming, and the watermark barrier.</summary>
    private readonly Warmer<TKey, TWarm> _warmer;

    /// <summary>
    ///     The watchdog period: a recurring timer wakes a sleeping consumer loop so it re-checks the state,
    ///     guarding against a missed readiness signal. <see cref="Timeout.InfiniteTimeSpan" /> disables it.
    /// </summary>
    private readonly TimeSpan _watchdogPeriod;

    /// <summary>The global accumulated weight currently held by the per-key buffers (the buffer budget).</summary>
    private long _totalWeight;

    /// <summary>
    ///     Initializes a new instance of the <see cref="WarmProcessor{TKey,TValue,TGroup,TWarm}" /> class.
    /// </summary>
    /// <param name="warmer">The ready-made warmer (key segmentation + concurrent warming + watermark barrier).</param>
    /// <param name="keySelector">Selects the warming key for each input value.</param>
    /// <param name="policy">Decides which keys require warming and receives the warm result.</param>
    /// <param name="accumulatorFactory">Creates the per-key accumulator buffers.</param>
    /// <param name="queueWeightLimit">The maximum total weight the per-key delayed buffers may hold.</param>
    /// <exception cref="ArgumentNullException">
    ///     Thrown when any of <paramref name="warmer" />, <paramref name="keySelector" />,
    ///     <paramref name="policy" /> or <paramref name="accumulatorFactory" /> is <see langword="null" />.
    /// </exception>
    /// <param name="watchdogPeriod">
    ///     The period of the recurring watchdog timer that periodically wakes a sleeping consumer loop so it
    ///     re-checks the state (a safety net against a missed readiness signal). Pass <see langword="null" />
    ///     or <see cref="Timeout.InfiniteTimeSpan" /> to disable the watchdog.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException">
    ///     Thrown when <paramref name="queueWeightLimit" /> is not positive.
    /// </exception>
    public WarmProcessor(
        Warmer<TKey, TWarm> warmer,
        MapSelector<TValue, TKey> keySelector,
        IWarmPolicy<TKey, TWarm> policy,
        IWarmAccumulatorFactory<TKey, TValue, TGroup> accumulatorFactory,
        long queueWeightLimit,
        TimeSpan watchdogPeriod)
    {
        ArgumentNullException.ThrowIfNull(warmer);
        ArgumentNullException.ThrowIfNull(keySelector);
        ArgumentNullException.ThrowIfNull(policy);
        ArgumentNullException.ThrowIfNull(accumulatorFactory);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(queueWeightLimit);

        _input = new EventTask<bool>();
        _output = new EventTask<bool>();

        _warmer = warmer;
        _keySelector = keySelector;
        _policy = policy;
        _accumulatorFactory = accumulatorFactory;
        _queueWeightLimit = queueWeightLimit;
        _watchdogPeriod = watchdogPeriod;

        _fanIn = new FanInSlim();
        _delayedQueue = new Dictionary<TKey, WarmAccumulator<TValue, TGroup>>();

        // Wake the loop when the input consumator has data or the output producer frees capacity.
        _input.OnReady += _fanIn.GetSignalCallback(InputSlot).Handler;
        _output.OnReady += _fanIn.GetSignalCallback(OutputSlot).Handler;
        _warmer.OnReady += _fanIn.GetSignalCallback(WarmSlot).Handler;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="WarmProcessor{TKey,TValue,TGroup,TWarm}" /> class
    ///     with the watchdog disabled.
    /// </summary>
    /// <param name="warmer">The ready-made warmer (key segmentation + concurrent warming + watermark barrier).</param>
    /// <param name="keySelector">Selects the warming key for each input value.</param>
    /// <param name="policy">Decides which keys require warming and receives the warm result.</param>
    /// <param name="accumulatorFactory">Creates the per-key accumulator buffers.</param>
    /// <param name="queueWeightLimit">The maximum total weight the per-key delayed buffers may hold.</param>
    /// <exception cref="ArgumentNullException">
    ///     Thrown when any of <paramref name="warmer" />, <paramref name="keySelector" />,
    ///     <paramref name="policy" /> or <paramref name="accumulatorFactory" /> is <see langword="null" />.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    ///     Thrown when <paramref name="queueWeightLimit" /> is not positive.
    /// </exception>
    public WarmProcessor(
        Warmer<TKey, TWarm> warmer,
        MapSelector<TValue, TKey> keySelector,
        IWarmPolicy<TKey, TWarm> policy,
        IWarmAccumulatorFactory<TKey, TValue, TGroup> accumulatorFactory,
        long queueWeightLimit) : this(warmer, keySelector, policy, accumulatorFactory, queueWeightLimit,
        Timeout.InfiniteTimeSpan)
    {
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="source"></param>
    /// <param name="output"></param>
    /// <param name="context"></param>
    public void Fuse(IAsyncConsumator<Watermarked<TValue>> source, IAsyncProducator<Unio<TValue, TGroup, Watermark>> output, FlowContext context)
    {
        context.RegisterBackground(() => InternalExecuteAsync(source, output, context));
    }

    /// <summary>
    ///     Creates the recurring watchdog timer that periodically wakes a sleeping consumer loop by
    ///     signaling <see cref="WatchdogSlot" />, so it re-checks the state even if a readiness signal
    ///     was missed. Returns <see langword="null" /> when the watchdog is disabled
    ///     (<see cref="Timeout.InfiniteTimeSpan" /> or a non-positive period).
    /// </summary>
    private ITimer? CreateWatchdog()
    {
        if (_watchdogPeriod == Timeout.InfiniteTimeSpan || _watchdogPeriod <= TimeSpan.Zero)
            return null;

        return TimeProvider.System.CreateTimer(
            _ => _fanIn.Signal(WatchdogSlot),
            null,
            _watchdogPeriod,
            _watchdogPeriod);
    }
}