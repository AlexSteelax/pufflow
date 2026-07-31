using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Steelax.Toolkit.HighPerformance.Concurrency.Primitives;
using Steelax.Toolkit.HighPerformance.Primitives;

namespace Steelax.Pufflow.Operators;

/// <summary>
/// Coordinates bounded concurrent warming of key segments with strict (watermark-ordered) emission.
/// </summary>
/// <typeparam name="TKey">The key type used to partition the stream for warming.</typeparam>
/// <typeparam name="TWarm">The warming data type produced by an <see cref="IAsyncJob{TKey,TWarm}"/>.</typeparam>
/// <remarks>
/// <para>
/// Keys are accumulated into fixed-capacity segments (sealed by size, or by <c>segmentLinger</c>
/// elapsed while the segment is idle). A segment is "open until run": it keeps accepting keys
/// until it is actually handed to a warming job. Jobs run on a bounded pool of concurrent workers
/// (<see cref="BitTaskAny"/>, at most <c>maxConcurrency</c>), while the segments themselves wait
/// in a bounded ring (<c>maxQueued</c>). Completed segments are extracted strictly in order
/// (head-of-line), preserving watermark monotonicity.
/// </para>
/// <para>
/// The consumer loop checks <see cref="CanAdd"/>, feeds via <see cref="AddKey"/>, and pumps work
/// via <see cref="WarmNext{TWarmable}"/> until it returns <see langword="false"/>, after which it
/// waits on the <see cref="OnReady"/> event (a job completion or the linger timer). Faults and
/// cancellations of a segment's job surface when that segment reaches the head.
/// </para>
/// </remarks>
public sealed class Warmer<TKey, TWarm> : IDisposable, IAsyncDisposable
{
    private readonly CancellationTokenSource _cts = new();
    private readonly BitTaskAny _taskAny;
    private readonly RingBuffer<JobSegment<TKey, TWarm>> _queue;
    private readonly ITimer _timer;
    private readonly IJobFactory<TKey, TWarm> _jobFactory;
    /// <summary>
    /// Raised when a warm job completes or the linger timer fires, so a consumer can wake and drain.
    /// </summary>
    [PublicAPI]
    public event Action? OnReady;
    private readonly TimeSpan _segmentLinger;
    private readonly int _segmentCapacity;
    private readonly Stack<JobSegment<TKey, TWarm>> _free = new();

    /// <summary>The number of segments from the head already handed to <see cref="_taskAny"/> (the run frontier).</summary>
    private int _assignedJobs;

    /// <summary>The linger-fired flag; read and reset atomically.</summary>
    private bool _lingerPending;

    /// <summary>Diagnostic: has Flush() been called (source exhausted) — used to spot seal refusals.</summary>
    private bool _flushPending;

    /// <summary>
    /// Initializes a new <see cref="Warmer{TKey,TWarm}"/>.
    /// </summary>
    /// <param name="maxConcurrency">The maximum number of concurrently running jobs (1..32).</param>
    /// <param name="maxQueued">The maximum number of segments buffered in the ring.</param>
    /// <param name="segmentCapacity">The maximum number of keys per segment.</param>
    /// <param name="segmentLinger">The idle interval after which a partial segment is sealed.</param>
    /// <param name="jobFactory">Factory used to create warming jobs.</param>
    /// <param name="timeProvider">The time provider used for the linger timer; defaults to <see cref="TimeProvider.System"/>.</param>
    public Warmer(
        int maxConcurrency,
        int maxQueued,
        int segmentCapacity,
        TimeSpan segmentLinger,
        IJobFactory<TKey, TWarm> jobFactory,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(jobFactory);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxConcurrency);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(maxConcurrency, BitTaskAny.MaxCapacity);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxQueued);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(segmentCapacity);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(segmentLinger, TimeSpan.Zero);

        _jobFactory = jobFactory;
        _segmentLinger = segmentLinger;
        _segmentCapacity = segmentCapacity;
        _taskAny = new BitTaskAny(OnSignal, maxConcurrency);
        _queue = new RingBuffer<JobSegment<TKey, TWarm>>(maxQueued);
        _timer = (timeProvider ?? TimeProvider.System).CreateTimer(
            _ =>
            {
                Volatile.Write(ref _lingerPending, true);
                OnReady?.Invoke();
            },
            null,
            Timeout.InfiniteTimeSpan,
            Timeout.InfiniteTimeSpan);
    }

    private void OnSignal() => OnReady?.Invoke();

    /// <summary>Takes a reusable segment from the free pool, or allocates a new one.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private JobSegment<TKey, TWarm> GetSegment()
    {
        return _free.TryPop(out var segment) ? segment : new JobSegment<TKey, TWarm>(_segmentCapacity);
    }

    /// <summary>Reuses the segment and returns it to the free pool.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ReturnSegment(JobSegment<TKey, TWarm> segment)
    {
        segment.Reuse();
        _free.Push(segment);
    }

    /// <summary>
    /// Indicates whether a new key can be accepted: either the ring has a free slot, or the
    /// current tail segment is still accepting keys.
    /// </summary>
    [PublicAPI]
    public bool CanAdd =>
        !_queue.IsFull ||
        (_queue.TryPeekTail(out var tail) && tail.CanAccept);

    /// <summary>Indicates whether the warmer has no segments in flight and no completed work left.</summary>
    [PublicAPI]
    public bool IsEmpty => _queue.IsEmpty && _taskAny.Count == 0;

    /// <summary>
    /// Indicates whether the job pool is full of already-completed jobs and no new job can be
    /// assigned until they are drained.
    /// </summary>
    [PublicAPI]
    public bool QueueFilled => _taskAny.CountReady == _taskAny.Capacity;

    /// <summary>
    /// Adds a key (with its watermark) to the current tail segment.
    /// </summary>
    /// <param name="key">The key to add.</param>
    /// <param name="watermark">The key's watermark; the segment keeps the maximum.</param>
    /// <remarks>
    /// Must be called only when <see cref="CanAdd"/> is <see langword="true"/> (the caller reacts
    /// to backpressure by draining completed segments first).
    /// </remarks>
    /// <exception cref="InvalidOperationException">Thrown when <see cref="CanAdd"/> is violated (unreachable in practice).</exception>
    [PublicAPI]
    public void AddKey(TKey key, Watermark watermark)
    {
        Debug.Assert(CanAdd, "AddKey requires CanAdd.");

        // Create a new segment when there is no tail (the ring is empty) or the tail no longer accepts keys.
        if (!_queue.TryPeekTail(out var tail) || !tail.CanAccept)
        {
            tail = GetSegment();

            if (!_queue.TryEnqueue(tail))
                throw new InvalidOperationException();
        }

        // Arm the linger timer with the segment's first element.
        if (!tail.HasAny)
            _timer.Change(_segmentLinger, Timeout.InfiniteTimeSpan);

        // Register the data.
        tail.Add(key, watermark);

        // The segment may have just filled — try to start it for warming right away.
        _ = AssignNextJob();
    }

    /// <summary>
    /// Pumps all segment work on the consumer loop: drains completed jobs, seals the tail when
    /// the linger interval elapsed, starts pending jobs, and applies the result of the next
    /// completed head segment (head-of-line).
    /// </summary>
    /// <param name="warmable">The sink that consumes per-key warming data on the loop thread.</param>
    /// <param name="keys">The keys warmed by the extracted segment, when one was available.</param>
    /// <param name="watermark">The maximum watermark of the extracted segment (may be <see cref="Watermark.Nothing()"/>).</param>
    /// <returns>
    /// <see langword="true"/> if a completed head segment was warmed; otherwise, <see langword="false"/>.
    /// </returns>
    /// <remarks>
    /// Called in a loop until it returns <see langword="false"/>, after which the caller waits on
    /// the <see cref="OnReady"/> event (a job completion or the linger timer). If the head segment's job
    /// faulted or was canceled, the exception is rethrown when the segment is extracted.
    /// </remarks>
    [PublicAPI]
    public bool WarmNext<TWarmable>(TWarmable warmable, [MaybeNullWhen(false)] out TKey[] keys, out Watermark watermark)
        where TWarmable : IWarmPolicy<TKey, TWarm>
    {
        // Free completed slots (the result stays in the segment).
        _ = DrainCompleted();

        // Linger may have fired — capture and reset the flag atomically.
        var linger = Interlocked.Exchange(ref _lingerPending, false);

        // Assign jobs; linger allows sealing the accepting tail.
        while (AssignNextJob(forceSeal: linger)) { }

        // If linger fired (including while processing) but the tail is still open
        // (no free slot), re-arm the timer so the signal repeats.
        if ((linger || Volatile.Read(ref _lingerPending)) && TryPeekAcceptingTail())
            _timer.Change(_segmentLinger, Timeout.InfiniteTimeSpan);
        
        if (_queue.IsEmpty && _taskAny.Count == 0)
            _free.Clear();

        var extracted = TryExtractHead(warmable, out keys, out watermark);

        return extracted;
    }

    /// <summary>
    /// Force-seals the accepting tail and starts pending jobs as slots free up. Used at
    /// end-of-stream to flush a partial tail that the linger timer has not sealed yet.
    /// </summary>
    /// <remarks>
    /// Call alongside <see cref="WarmNext{TWarmable}"/> until <see cref="IsEmpty"/> becomes
    /// <see langword="true"/>. If the job pool is full, the tail is sealed as soon as a slot frees.
    /// </remarks>
    [PublicAPI]
    public void Flush()
    {
        Volatile.Write(ref _flushPending, true);
        while (AssignNextJob(forceSeal: true)) { }
    }

    /// <summary>Frees completed slots in <see cref="_taskAny"/>; the result stays in the segment.</summary>
    /// <returns>The number of completed slots freed.</returns>
    private int DrainCompleted()
    {
        var count = 0;
        while (_taskAny.TryTake(out _, out _))
            count++;
        return count;
    }

    /// <summary>Returns the accepting tail, if any (i.e. there is still a segment to seal).</summary>
    private bool TryPeekAcceptingTail()
    {
        return _queue.TryPeekTail(out var tail) && tail.CanAccept;
    }

    /// <summary>
    /// Assigns a job to the next unassigned segment (in order from the head).
    /// </summary>
    /// <param name="forceSeal">
    /// Allows sealing an accepting (partially filled) segment — used by the linger timer.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when a job was assigned and the loop should continue; otherwise <see langword="false"/>.
    /// </returns>
    private bool AssignNextJob(bool forceSeal = false)
    {
        // After Flush() the source is exhausted, so an accepting tail must be sealed as soon as a
        // task slot frees up — otherwise the partial tail would never be assigned or drained.
        var seal = forceSeal || Volatile.Read(ref _flushPending);

        if (!_taskAny.CanAdd || _assignedJobs == _queue.Count)
            return false;

        // Get the next unassigned segment in the queue.
        if (!_queue.TryGetAt(_assignedJobs, out var job))
            throw new InvalidOperationException();

        // The segment still accepts keys and is not sealed — nothing to assign, stop.
        if (job.CanAccept && !seal)
            return false;

        var task = job.RunJob(_jobFactory, _cts.Token);
        _taskAny.Insert(task);
        _assignedJobs++;
        return true;
    }

    /// <summary>Extracts the completed head and applies its result (strict head-of-line emission).</summary>
    private bool TryExtractHead<TWarmable>(TWarmable warmable, [MaybeNullWhen(false)] out TKey[] keys, out Watermark watermark)
        where TWarmable : IWarmPolicy<TKey, TWarm>
    {
        var headExist = _queue.TryPeekHead(out var head);
        if (headExist && head is not null && head.IsJobCompleted)
        {
            Debug.Assert(_assignedJobs > 0, "A completed head must have been assigned.");

            keys = head.ApplyResult(warmable);
            watermark = head.Watermark;

            // The head was already observed completed — dequeue must succeed.
            _ = _queue.TryDequeue(out _);
            _assignedJobs--;

            ReturnSegment(head);

            return true;
        }

        keys = null;
        watermark = Watermark.Nothing();
        return false;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _timer.Dispose();
        _cts.Cancel();
        DisposeJobs();
        _cts.Dispose();
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        _timer.Dispose();
        await _cts.CancelAsync();
        DisposeJobs();
        _cts.Dispose();
    }

    /// <summary>Disposes the jobs of the live segments in the ring.</summary>
    private void DisposeJobs()
    {
        // Segments in _free have already been reused (_job == null) — dispose only the live ring segments.
        for (var i = 0; i < _queue.Count; i++)
            _queue[i].Dispose();
    }
}
