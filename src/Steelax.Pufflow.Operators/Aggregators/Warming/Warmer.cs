using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Steelax.Pufflow.Operators.Common;
using Steelax.Toolkit.HighPerformance.Concurrency.Primitives;
using Steelax.Toolkit.HighPerformance.Primitives;

namespace Steelax.Pufflow.Operators.Aggregators.Warming;

/// <summary>
///     Coordinates bounded concurrent warming of key segments with strict (watermark-ordered) emission.
/// </summary>
/// <typeparam name="TKey">The key type used to partition the stream for warming.</typeparam>
/// <typeparam name="TWarm">The warming data type produced by an <see cref="IAsyncJob{TKey,TWarm}" />.</typeparam>
/// <remarks>
///     <para>
///         Keys are accumulated into fixed-capacity segments, which are handed to a warming job once they are
///         full (or explicitly sealed via <see cref="WarmMode.SealTail" />). A segment is "open until run":
///         it keeps accepting keys until a job actually starts over it. Jobs run on a bounded pool of concurrent
///         workers (<see cref="BitTaskAny" />, at most <c>maxConcurrency</c>), while the segments themselves
///         wait in a bounded ring (<c>maxQueued</c>). Completed segments are extracted strictly in order
///         (head-of-line), preserving watermark monotonicity.
///     </para>
///     <para>
///         The consumer loop checks <see cref="CanAdd" />, feeds via <see cref="AddKey" />, and pumps work
    ///         via <see cref="WarmNext" /> until it returns <see langword="false" />, after which it
///         waits on the <see cref="OnReady" /> event (a job completion). A <see cref="WarmMode.SealTail" /> pass
///         attempts to start a partially filled tail when a job slot is free; it carries no guarantee — if the
///         pool is busy the tail stays open (and keeps accepting keys) until a slot frees up on a later pass.
///         Faults and cancellations of a segment's job surface when that segment reaches the head.
///     </para>
/// </remarks>
internal sealed class Warmer<TKey, TWarm> : IAsyncDisposable
    where TKey : notnull
{
    private readonly CancellationTokenSource _cts = new();
    private readonly Stack<JobSegment<TKey, TWarm>> _free = new();
    private readonly IJobFactory<TKey, TWarm> _jobFactory;
    private readonly Deque<JobSegment<TKey, TWarm>> _queue;
    private readonly BitTaskAny _taskAny;

    /// <summary>The number of segments from the head already handed to <see cref="_taskAny" /> (the run frontier).</summary>
    private int _assignedJobs;
    private readonly int _segmentCapacity;

    /// <summary>
    ///     Initializes a new <see cref="Warmer{TKey,TWarm}" />.
    /// </summary>
    /// <param name="maxConcurrency">The maximum number of concurrently running jobs (1..32).</param>
    /// <param name="maxQueued">The maximum number of segments buffered in the ring.</param>
    /// <param name="segmentCapacity">The maximum number of keys per segment.</param>
    /// <param name="jobFactory">Factory used to create warming jobs.</param>
    public Warmer(
        int maxConcurrency,
        int maxQueued,
        int segmentCapacity,
        IJobFactory<TKey, TWarm> jobFactory)
    {
        ArgumentNullException.ThrowIfNull(jobFactory);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxConcurrency);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(maxConcurrency, BitTaskAny.MaxCapacity);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxQueued);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(segmentCapacity);

        _jobFactory = jobFactory;
        _segmentCapacity = segmentCapacity;
        _taskAny = new BitTaskAny(OnSignal, maxConcurrency);
        _queue = new Deque<JobSegment<TKey, TWarm>>(maxQueued);
    }

    /// <summary>
    ///     Indicates whether a new key can be accepted: either the ring has a free slot, or the
    ///     current tail segment is still accepting keys.
    /// </summary>
    [PublicAPI]
    public bool CanAdd =>
        !_queue.IsFull ||
        (_queue.TryPeekLast(out var tail) && tail.CanAdvance);

    /// <summary>Indicates whether the warmer has no segments in flight and no completed work left.</summary>
    [PublicAPI]
    public bool IsEmpty => _queue.IsEmpty && _taskAny.Count == 0;

    /// <summary>
    ///     Indicates whether the job pool is full of already-completed jobs and no new job can be
    ///     assigned until they are drained.
    /// </summary>
    [PublicAPI]
    public bool QueueFilled => _taskAny.CountReady == _taskAny.Capacity;

    [PublicAPI]
    public bool IsSinglePartial => _taskAny.Count == 0 && _queue.Count == 1;

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        
        // Segments in _free have already been reused (their job was disposed on apply) — dispose only the live ring segments.
        while (_queue.TryPopLast(out var segment))
            await segment.DisposeAsync();

        _cts.Dispose();
    }

    /// <summary>
    ///     Raised when a warm job completes, so a consumer can wake and drain.
    /// </summary>
    [PublicAPI]
    public event Action? OnReady;

    private void OnSignal()
    {
        OnReady?.Invoke();
    }

    /// <summary>Takes a reusable segment from the free pool, or allocates a new one.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private JobSegment<TKey, TWarm> GetSegment()
    {
        return _free.TryPop(out var segment) ? segment : new JobSegment<TKey, TWarm>(_segmentCapacity);
    }

    /// <summary>Returns the segment (already reset by its <see cref="JobSegment{TKey,TWarm}.ApplyResult" />) to the free pool.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ReturnSegment(JobSegment<TKey, TWarm> segment)
    {
        _free.Push(segment);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Truncate()
    {
        if (_free.Count != 0 && _queue.IsEmpty && _taskAny.Count == 0)
            _free.Clear();
    }

    /// <summary>
    ///     Adds a key (with its watermark) to the current tail segment. Creates a new segment when there is
    ///     no tail or the tail is no longer accepting keys.
    /// </summary>
    /// <param name="key">The key to add.</param>
    /// <param name="watermark">The key's watermark; the segment keeps the maximum.</param>
    /// <remarks>
    ///     Must be called only when <see cref="CanAdd" /> is <see langword="true" /> (the caller reacts
    ///     to backpressure by draining completed segments first).
    /// </remarks>
    /// <exception cref="InvalidOperationException">Thrown when the ring is full (backpressure was ignored).</exception>
    [PublicAPI]
    public void AddKey(TKey key, Watermark watermark)
    {
        Debug.Assert(CanAdd, "AddKey requires CanAdd.");

        // Create a new segment when there is no tail or the tail no longer accepts keys.
        if (!_queue.TryPeekLast(out var tail) || !tail.CanAdvance)
        {
            tail = GetSegment();

            if (!_queue.TryAddLast(tail))
                throw new InvalidOperationException();
        }

        // Register the data.
        tail.Advance(key, watermark);

        // The segment may have just filled — try to start it for warming right away.
        AssignNextJobs();
    }

    /// <summary>
    ///     Advances the covering watermark of the current accepting tail segment so it reflects the maximum watermark
    ///     of all the data it ultimately covers (including later, hotter events for keys that are already registered
    ///     there). A no-op when there is no accepting tail segment or that segment has already been sealed/running.
    /// </summary>
    /// <param name="watermark">The newer watermark to fold into the segment, if it advances it.</param>
    [PublicAPI]
    public void AdvanceOpenWatermark(Watermark watermark)
    {
        if (_queue.TryPeekLast(out var tail) && tail.CanAdvance)
            tail.Advance(watermark);
    }

    /// <summary>
    ///     Pumps all segment work on the consumer loop: drains completed jobs, starts sealable/pending jobs
    ///     in order from the head, and applies the result of the next completed head segment (head-of-line).
    /// </summary>
    /// <param name="mode">
    ///     The sealing policy for the current open tail: <see cref="WarmMode.Normal" /> leaves it accepting,
    ///     <see cref="WarmMode.SealTail" /> attempts to start a partially filled tail when a job slot is free.
    /// </param>
    /// <param name="policy">The sink that consumes per-key warming data on the loop thread.</param>
    /// <param name="keys">The keys warmed by the extracted segment, when one was available.</param>
    /// <param name="watermark">The maximum watermark of the extracted segment (may be <see cref="Watermark.Nothing()" />).</param>
    /// <returns>
    ///     <see langword="true" /> if a completed head segment was warmed; otherwise, <see langword="false" />.
    /// </returns>
    /// <remarks>
    ///     Called in a loop until it returns <see langword="false" />, after which the caller waits on
    ///     the <see cref="OnReady" /> event (a job completion). If the head segment's job faulted or was
    ///     canceled, the exception is rethrown when the segment is extracted.
    /// </remarks>
    [PublicAPI]
    public bool WarmNext(IWarmPolicy<TKey, TWarm> policy, WarmMode mode, [MaybeNullWhen(false)] out TKey[] keys, out Watermark watermark)
    {
        // Free completed slots (the result stays in the segment).
        // Assign jobs; SealTail lets the accepting tail start even while partially filled.
        AssignNextJobs(mode == WarmMode.SealTail);

        // Strict head-of-line emission: apply the result of the next completed head segment.
        if (TryExtractCompleted(out var segment))
        {
            watermark = segment.Watermark;
            keys = segment.ApplyResult(policy);

            // ApplyResult already reset the segment; return it to the free pool.
            ReturnSegment(segment);

            return true;
        }
        
        Truncate();

        keys = null;
        watermark = Watermark.Nothing();
        return false;
    }

    /// <summary>
    ///     Progresses only the warming machinery — assigns jobs to pending segments (draining completed job
    ///     slots as needed) — without extracting or emitting any completed segment. Used when the warmer's
    ///     output capacity is unavailable and the caller wants to keep the warming pipeline moving (e.g.
    ///     sealing a tail, running full segments) without touching the output. Unlike <see cref="WarmNext" />,
    ///     it returns nothing and does not apply any result.
    /// </summary>
    /// <param name="mode">
    ///     The sealing policy for the current open tail, as in <see cref="WarmNext" /> — see
    ///     <see cref="WarmMode" />.
    /// </param>
    [PublicAPI]
    public void Progress(WarmMode mode)
    {
        AssignNextJobs(mode == WarmMode.SealTail);
        
        Truncate();
    }

    /// <summary>
    ///     Assigns a job to the next unassigned segment (in order from the head).
    /// </summary>
    /// <param name="forceSeal">
    ///     Allows starting an accepting (partially filled) segment — used by <see cref="WarmMode.SealTail" />.
    /// </param>
    private void AssignNextJobs(bool forceSeal = false)
    {
        // Frees completed slots. The result stays in the segment.
        while (_taskAny.TryTake(out _, out _)) ;
        
        while (_taskAny.CanAdd)
        {
            // Get the next unassigned segment in the queue.
            if (!_queue.TryGetAt(_assignedJobs, out var job))
                break;

            // The segment is still open and not sealed — nothing to assign, stop.
            if (job.CanAdvance && !forceSeal)
                break;

            var task = job.RunJob(_jobFactory, _cts.Token);
            _taskAny.Insert(task);
            _assignedJobs++;
        }
    }

    /// <summary>
    ///     Dequeues the next completed head segment (strict head-of-line), without applying its result.
    ///     The caller proceeds to apply the result via <see cref="JobSegment{TKey,TWarm}.ApplyResult" /> once
    ///     the segment is out; the caller is also responsible for returning it to the pool. A no-op when
    ///     the head is not yet complete.
    /// </summary>
    /// <param name="segment">The completed head segment, when one was available.</param>
    /// <returns>
    ///     <see langword="true" /> when a completed head segment was extracted; otherwise <see langword="false" />.
    /// </returns>
    private bool TryExtractCompleted([MaybeNullWhen(false)] out JobSegment<TKey, TWarm> segment)
    {
        if (_queue.TryPeekFirst(out var head) && head.IsJobCompleted)
        {
            Debug.Assert(_assignedJobs > 0, "A completed head must have been assigned.");

            // The head was already observed completed — dequeue must succeed.
            _ = _queue.TryPopFirst(out _);
            _assignedJobs--;

            segment = head;

            return true;
        }

        segment = null;
        return false;
    }
}