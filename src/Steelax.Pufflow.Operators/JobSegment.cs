using System.Runtime.CompilerServices;

namespace Steelax.Pufflow.Operators;

/// <summary>
/// A reusable key segment buffered in the <see cref="Warmer{TKey,TWarm}"/> ring.
/// </summary>
/// <typeparam name="TKey">The key type used to partition the stream for warming.</typeparam>
/// <typeparam name="TWarm">The warming data type produced by an <see cref="IAsyncJob{TKey,TWarm}"/>.</typeparam>
/// <remarks>
/// <para>
/// The segment holds a fixed-capacity key buffer and the running <see cref="IAsyncJob{TKey,TWarm}"/>.
/// It has no stored state flags: every property is derived from the started task and the fill
/// count, so sealing (readiness to run) is tracked entirely by the <see cref="Warmer{TKey,TWarm}"/>.
/// </para>
/// <para>
/// Segments are recycled through the warmer's free pool: <see cref="Reuse"/> resets the segment
/// after its result has been applied.
/// </para>
/// </remarks>
internal sealed class JobSegment<TKey, TWarm>(int capacity) : IDisposable
{
    private static readonly bool IsReferenceOrContainsReferences = RuntimeHelpers.IsReferenceOrContainsReferences<TKey>();

    private IAsyncJob<TKey, TWarm>? _job;
    private Task? _task;
    private TKey[]? _keysSnapshot;

    private Watermark _watermark = Watermark.Nothing();
    private readonly TKey[] _keys = new TKey[capacity];
    private int _count;

    /// <summary>The maximum watermark of the keys added to the segment.</summary>
    [PublicAPI]
    public Watermark Watermark => _watermark;

    /// <summary>
    /// Applies the completed job result to the <paramref name="warmable"/> sink, rethrowing the
    /// job fault if any, and disposes the underlying job.
    /// </summary>
    /// <typeparam name="TCollection">The concrete sink type (a struct avoids boxing).</typeparam>
    /// <param name="warmable">The sink that consumes per-key warming data.</param>
    /// <returns>The keys warmed by the segment (the snapshot handed to the job).</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the segment's job has not been started or has not completed.
    /// </exception>
    [PublicAPI]
    public TKey[] ApplyResult<TCollection>(TCollection warmable)
        where TCollection : IWarmPolicy<TKey, TWarm>
    {
        Debug.Assert(_job is not null);
        Debug.Assert(_task is not null);

        if (_job is null || _task is not { IsCompleted: true })
            throw new InvalidOperationException("The segment job has not completed.");

        try
        {
            _task.GetAwaiter().GetResult();

            foreach (var (key, warm) in _job.GetResult())
                warmable.OnWarmed(key, warm);

            _job.SynchronousComplete();
        }
        finally
        {
            _job.Dispose();
        }
        
        return _keysSnapshot!;
    }

    /// <summary>Creates a job via the factory and starts it over the filled keys.</summary>
    /// <param name="jobFactory">The factory used to create the job.</param>
    /// <param name="cancellationToken">Cancels the warming work.</param>
    /// <returns>The started warming task.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the segment's job has already run.</exception>
    [PublicAPI]
    public Task RunJob(IJobFactory<TKey, TWarm> jobFactory, CancellationToken cancellationToken)
    {
        if (_job is not null)
            throw new InvalidOperationException("The segment job has run already.");

        // Keys snapshot: the job owns its own list, so the internal buffer can be cleared safely.
        _keysSnapshot = new TKey[_count];
        Array.Copy(_keys, _keysSnapshot, _count);

        _job = jobFactory.CreateAsyncJob();
        _task = _job.ExecuteAsync(_keysSnapshot, cancellationToken);
        return _task;
    }

    /// <summary>Indicates whether the segment can still accept keys (not running and not full).</summary>
    [PublicAPI]
    public bool CanAccept => _task is null && _keys.Length != _count;

    /// <summary>Indicates whether the segment contains at least one key.</summary>
    [PublicAPI]
    public bool HasAny => _count > 0;

    /// <summary>Indicates whether the segment has been handed to a job (running or completed).</summary>
    [PublicAPI]
    public bool IsJobAssigned => _task is not null;

    /// <summary>Indicates whether the segment's job has completed (success, fault or cancellation).</summary>
    [PublicAPI]
    public bool IsJobCompleted => _task is { IsCompleted: true };

    /// <summary>Adds a key (with its watermark) to the segment buffer.</summary>
    /// <param name="key">The key to add.</param>
    /// <param name="watermark">The key's watermark; the segment keeps the maximum.</param>
    /// <exception cref="InvalidOperationException">Thrown when the segment is full or running.</exception>
    [PublicAPI]
    public void Add(TKey key, Watermark watermark)
    {
        if (!CanAccept)
            throw new InvalidOperationException("Cannot add a key to a running or full segment.");

        _keys[_count++] = key;

        if (watermark > _watermark)
            _watermark = watermark;
    }

    /// <summary>Resets the segment for reuse in the ring.</summary>
    [PublicAPI]
    public void Reuse()
    {
        if (IsReferenceOrContainsReferences)
            Array.Clear(_keys);

        _keysSnapshot = null;
        _task = null;
        _job = null;
        
        _count = 0;
        _watermark = Watermark.Nothing();
    }
    
    /// <summary>Disposes the underlying job, if any.</summary>
    [PublicAPI]
    public void Dispose()
    {
        // The task may still be running; Task.Dispose() is only allowed on completed tasks, so the
        // task lifecycle is managed by BitTaskAny/GC rather than the segment.
        _job?.Dispose();
    }
}
