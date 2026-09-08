using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Aggregators.Warming;

/// <summary>
///     A reusable key segment buffered in the warmer's ring.
/// </summary>
/// <typeparam name="TKey">The key type used to partition the stream for warming.</typeparam>
/// <typeparam name="TWarm">The warming data type produced by an <see cref="IAsyncJob{TKey,TWarm}" />.</typeparam>
/// <remarks>
///     <para>
///         The segment holds a fixed-capacity key buffer, the reusable result buffer (backed by the
///         <see cref="ResultHolder{TKey,TWarm}" /> handed to the job) and the running
///         <see cref="IAsyncJob{TKey,TWarm}" />. It has no stored state flags: every property is derived
///         from the started task and the fill count, so sealing (readiness to run) is tracked entirely by
///         the warmer's ring.
///     </para>
///     <para>
///         Segments are recycled through the warmer's free pool: <see cref="ApplyResult" /> applies the
///         completed job's result to the policy and resets the segment (via <see cref="Reuse" />) so it can
///         be returned to the pool by the warmer.
///     </para>
/// </remarks>
internal sealed class JobSegment<TKey, TWarm>(int capacity) : IAsyncDisposable
    where TKey : notnull
{
    private static readonly bool IsReferenceOrContainsReferences = RuntimeHelpers.IsReferenceOrContainsReferences<TKey>();

    private readonly TKey[] _keys = new TKey[capacity];
    private int _count;

    private readonly Dictionary<TKey, TWarm> _holder = new(capacity);
    private IAsyncJob<TKey, TWarm>? _job;
    private Task? _task;

    /// <summary>The maximum watermark of the keys added to the segment.</summary>
    [PublicAPI]
    public Watermark Watermark { get; private set; } = Watermark.Nothing();

    /// <summary>Indicates whether the segment can still accept keys (not running and not full).</summary>
    [PublicAPI]
    public bool CanAdvance => _keys.Length != _count && _task is null;

    /// <summary>Indicates whether the segment contains at least one key.</summary>
    [PublicAPI]
    public bool HasAny => _count > 0;

    /// <summary>Indicates whether the segment has been handed to a job (running or completed).</summary>
    [PublicAPI]
    public bool IsJobAssigned => _task is not null;

    /// <summary>Indicates whether the segment's job has completed (success, fault or cancellation).</summary>
    [PublicAPI]
    public bool IsJobCompleted => _task is { IsCompleted: true };

    /// <summary>
    ///     Applies the completed job result to the <paramref name="policy" /> sink and recycles the segment.
    ///     For every key of the segment the policy is notified: with its warm data when the job recorded a
    ///     result for it, or without data when it did not (no warm produced for the key). A job fault, if
    ///     any, is rethrown. The segment is reset via <see cref="Reuse" /> in all cases (success, fault or
    ///     cancellation), so it returns to the pooling state clean of job and task references.
    /// </summary>
    /// <param name="policy">The sink that consumes per-key warming data.</param>
    /// <returns>
    ///     The keys handed to the job (the snapshot), so the caller can drain the corresponding delayed
    ///     values; the segment's internal buffer has already been reused and must not be touched.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    ///     Thrown when the segment's job has not been started or has not completed.
    /// </exception>
    [PublicAPI]
    public TKey[] ApplyResult(IWarmPolicy<TKey, TWarm> policy)
    {
        ThrowIfSegmentTrue(!IsJobCompleted, SegmentNotCompleted);

        var snapshot = KeySnapshot;
        
        try
        {
            _task!.GetAwaiter().GetResult();

            for (var i = 0; i < _count; i++)
            {
                var key = _keys[i];
                
                if (_holder.TryGetValue(key, out var warm))
                    policy.OnWarmed(key, warm);
                else
                    policy.OnWarmed(key);
            }
        }
        finally
        {
            Reuse();
        }

        return snapshot;
    }

    /// <summary>
    ///     Creates a job via the factory and starts it over a snapshot of the segment's keys, handing the
    ///     job a <see cref="ResultHolder{TKey,TWarm}" /> backed by the reusable result buffer. The job writes
    ///     its per-key warm data into the holder; keys without a recorded result are reported by
    ///     <see cref="ApplyResult" /> as warmed without data.
    /// </summary>
    /// <param name="jobFactory">The factory used to create the job.</param>
    /// <param name="cancellationToken">Cancels the warming work.</param>
    /// <returns>The started warming task.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the segment's job has already run.</exception>
    [PublicAPI]
    public Task RunJob(IJobFactory<TKey, TWarm> jobFactory, CancellationToken cancellationToken)
    {
        if (_job is not null)
            throw new InvalidOperationException("The segment job has run already.");

        _job = jobFactory.CreateAsyncJob();
        _task = _job.ExecuteAsync(KeySnapshot, new ResultHolder<TKey, TWarm>(_holder), cancellationToken);
        return _task;
    }

    /// <summary>Adds a key (with its watermark) to the segment buffer.</summary>
    /// <param name="key">The key to add.</param>
    /// <param name="watermark">The key's watermark; the segment keeps the maximum.</param>
    /// <exception cref="InvalidOperationException">Thrown when the segment is full or running.</exception>
    [PublicAPI]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Advance(TKey key, Watermark watermark)
    {
        ThrowIfSegmentTrue(!CanAdvance, SegmentAdvanceNotAllow);
        
        Debug.Assert(!_keys.AsSpan(0, _count).Contains(key), "Key already exists.");

        _keys[_count++] = key;

        if (watermark > Watermark)
            Watermark = watermark;
    }

    /// <summary>
    ///     Advances the segment's covering watermark when a newer event for a key that already lives in this
    ///     accepting segment arrives. The segment must reflect the maximum watermark of all the data it ultimately
    ///     covers; no second registration of the key is needed.
    /// </summary>
    /// <param name="watermark">The newer watermark to fold into the segment, if it advances it.</param>
    [PublicAPI]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Advance(Watermark watermark)
    {
        ThrowIfSegmentTrue(!CanAdvance, SegmentAdvanceNotAllow);

        if (watermark > Watermark)
            Watermark = watermark;
    }

    /// <summary>
    ///     Resets the segment for reuse in the warmer's free pool: clears the key and result buffers,
    ///     detaches the completed job/task and disposes the underlying job. Called from
    ///     <see cref="ApplyResult" /> after a completed job, so the segment is clean when it returns to
    ///     the pool. Must not be invoked on a segment whose job is still running — its result would be
    ///     discarded while the job may still write into the holder.
    /// </summary>
    private void Reuse()
    {
        if (IsReferenceOrContainsReferences)
            Array.Clear(_keys);

        var job = _job;
        
        _holder.Clear();

        _task = null;
        _job = null;
        _count = 0;
        
        Watermark = Watermark.Nothing();
        
        job!.Dispose();
    }

    #region helpers

    private const string SegmentAdvanceNotAllow = "Cannot add a key to a running or full segment.";
    private const string SegmentNotCompleted = "The segment job has not completed.";
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ThrowIfSegmentTrue([DoesNotReturnIf(false)] bool state, string message)
    {
        if (state)
            throw new InvalidOperationException(message);
    }

    /// <summary>
    ///     Flattens an exception and its innermost cause into a single diagnostic line, so a fault surfaced
    ///     from a canceled/failing job is reported with the root cause rather than only the outer wrapper.
    /// </summary>
    /// <param name="ex">The exception to unwrap.</param>
    /// <returns>A single-line diagnostic describing the exception chain and the innermost stack.</returns>
    private static string UnwrapDiagnostic(Exception ex)
    {
        var chain = new List<string>(4);
        for (var current = ex; current is not null; current = current.InnerException)
            chain.Add($"{current.GetType().Name}: {current.Message}");

        var innermost = ex;
        while (innermost.InnerException is not null)
            innermost = innermost.InnerException;

        return $"{string.Join(" -> ", chain)}{Environment.NewLine}{innermost.StackTrace}";
    }
    
    /// <summary>Returns a defensive copy of the buffered keys; the job owns this array for its lifetime.</summary>
    private TKey[] KeySnapshot
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            if (_count == 0)
                return [];
            
            var keys = new TKey[_count];
            Array.Copy(_keys, keys, _count);
            return keys;
        }
    }
    
    #endregion

    /// <summary>
    ///     Disposes the underlying job, if any. Intended for shutdown paths (cancellation or a fatal
    ///     error) when the warmer's ring is torn down; the normal success paths recycle segments through
    ///     <see cref="ApplyResult" /> / <see cref="Reuse" /> instead. Safe when the segment was already
    ///     reused (<c>_job</c> is <see langword="null" />).
    /// </summary>
    [PublicAPI]
    public async ValueTask DisposeAsync()
    {
        if (_job is null)
            return;

        // The task may still be running; Task.Dispose() is only allowed on completed tasks, so the
        // task lifecycle is managed by BitTaskAny/GC rather than the segment.
        switch (_task)
        {
            // Just clean state
            case { IsCompleted: true } or null:
                Reuse();
                break;
            case { IsCompleted: false }:
                try
                {
                    await _task;
                }
                catch (Exception ex)
                {
                    // A canceled task surfaces as OperationCanceledException during shutdown — expected, so
                    // swallow it; any other fault is logged. Reuse() below always runs (via finally) to free
                    // the segment regardless of the outcome.
                    if (ex is not OperationCanceledException)
                        Debug.Print(UnwrapDiagnostic(ex));
                }
                finally
                {
                    Reuse();
                }
                break;
        }
    }
}