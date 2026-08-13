using Steelax.Pufflow.Bridges;
using Steelax.Toolkit.HighPerformance.Concurrency.Primitives;

namespace Steelax.Pufflow.Operators;

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
public sealed partial class WarmProcessor<TKey, TValue, TGroup, TWarm>
    where TKey : notnull
{
    /// <summary>The fan-in slot signaled by the <see cref="Warmer{TKey,TWarm}" /> when work completes.</summary>
    private const int WarmSlot = 1;

    private const int ResultSlot = 2;

    /// <summary>The fan-in slot used only to wake a sleeping loop on cancellation (the source bridge uses slot 31).</summary>
    private const int CancellationSlot = 0;

    /// <summary>The fan-in slot signaled periodically by the watchdog timer to re-check a sleeping loop.</summary>
    private const int WatchdogSlot = 3;

    /// <summary>Creates the per-key accumulator buffers.</summary>
    private readonly IWarmAccumulatorFactory<TKey, TValue, TGroup> _accumulatorFactory;

    /// <summary>The bounded output buffer bridging the consumer loop and the downstream enumerator.</summary>
    private readonly InternalEventQueue<Unio<TValue, TGroup, Watermark>> _buffer;

    /// <summary>The per-key delayed buffers (the FlowGate dictionary).</summary>
    private readonly Dictionary<TKey, WarmAccumulator<TValue, TGroup>> _delayedQueue;

    /// <summary>The shared fan-in multiplexing source readiness and warm completion.</summary>
    private readonly FanInSlim _fanIn;

    /// <summary>Selects the warming key for a value.</summary>
    private readonly KeySelector<TValue, TKey> _keySelector;

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
        KeySelector<TValue, TKey> keySelector,
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

        _warmer = warmer;
        _keySelector = keySelector;
        _policy = policy;
        _accumulatorFactory = accumulatorFactory;
        _queueWeightLimit = queueWeightLimit;
        _watchdogPeriod = watchdogPeriod;

        _fanIn = new FanInSlim();
        _delayedQueue = new Dictionary<TKey, WarmAccumulator<TValue, TGroup>>();

        _buffer = new InternalEventQueue<Unio<TValue, TGroup, Watermark>>(256);

        _buffer.OnWriteReady += _fanIn.GetSignalCallback(ResultSlot).Handler;
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
        KeySelector<TValue, TKey> keySelector,
        IWarmPolicy<TKey, TWarm> policy,
        IWarmAccumulatorFactory<TKey, TValue, TGroup> accumulatorFactory,
        long queueWeightLimit) : this(warmer, keySelector, policy, accumulatorFactory, queueWeightLimit,
        Timeout.InfiniteTimeSpan)
    {
    }

    /// <summary>
    ///     Returns the output as an <see cref="IAsyncConsumator{T}" />, starting the processing loop on a
    ///     background task.
    /// </summary>
    [PublicAPI]
    public IAsyncConsumator<Unio<TValue, TGroup, Watermark>> GetAsyncConsumator(
        IAsyncEnumerator<Watermarked<TValue>> source, FlowContext context)
    {
        var consumator =
            new AsyncEnumeratorToAsyncConsumator<Watermarked<TValue>, IAsyncEnumerator<Watermarked<TValue>>>(source,
                _fanIn);
        _ = Task.Run(() => InternalExecuteAsync(consumator, context));
        return _buffer;
    }

    private async Task InternalExecuteAsync<TConsumator>(TConsumator consumator, FlowContext context)
        where TConsumator : IAsyncConsumator<Watermarked<TValue>>, ICursorable<Watermarked<TValue>>
    {
        // FanInSlim does not accept a CancellationToken: on cancellation we signal a dedicated slot to
        // wake the loop sleeping on _fanIn.WaitAsync(). The loop observes the token and exits.
        await using var cancellation = context.Token.Register(() => _fanIn.Signal(CancellationSlot));

        // The periodic watchdog wakes the sleeping loop so it re-checks the state — a safety net against
        // a missed readiness signal. It lives for the duration of the loop (disposed in the finally block).
        await using var watchdog = CreateWatchdog();

        var sourceCompleted = false;

        try
        {
            while (!context.Token.IsCancellationRequested)
            {
                // 1. Drain warmed segments (frees output capacity, delayed weight and pumps the warmer).
                var drain = DrainWarm();

                // 2. Handle the current source value, unless the source has already completed.
                var result = FlowResult.Idle;

                if (!sourceCompleted)
                {
                    if (consumator.TryPeek(out var item, out var completed))
                    {
                        result = TryHandleValue(in item);

                        if (result == FlowResult.Success)
                            consumator.Advance(); // the value was fully handled — move to the next
                    }
                    else if (completed)
                    {
                        // End of source: seal the tail segment and start pending jobs. From now on the loop
                        // only drains until the delayed queue and the progress watermark are fully emitted.
                        sourceCompleted = true;
                    }
                    // Nothing — the source is not ready yet; fall through to Idle.
                }

                // Once the source has completed, seal the tail segment and assign jobs on every iteration:
                // if all warmer slots were busy at the first Flush, the partial tail stays unassigned, and
                // AssignNextJob(forceSeal:false) from WarmNext will not seal it — otherwise the last segment
                // would never start and the loop would hang.
                if (sourceCompleted)
                    _warmer.Flush();

                // 3. Emit the held progress watermark once all delayed data has been drained.
                if (sourceCompleted && !TryFlushWatermark())
                    drain = FlowResult.OutputBlocked;

                // 4. Completion: everything has been drained and emitted.
                if (sourceCompleted && _warmer.IsEmpty && _delayedQueue.Count == 0) break;

                // 5. Combine and decide: retry immediately or plan waits and sleep on the fan-in.
                var combined = result;
                if (drain == FlowResult.OutputBlocked)
                    combined = FlowResult.OutputBlocked;
                else if (drain == FlowResult.Success)
                    combined = FlowResult.Success;

                if (PrepareWait(combined))
                    continue;

                await _fanIn.WaitAsync();
                _fanIn.Take();
            }
        }
        finally
        {
            // Always complete the buffer — on normal completion, cancellation and exceptions alike.
            // Otherwise the external reader would hang on MoveNextAsync, never receiving the end-of-stream signal.
            _buffer.Complete();
        }
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