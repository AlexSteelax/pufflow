using Steelax.Pufflow.Runtime;

namespace Steelax.Pufflow;

/// <summary>
///     Manages the lifecycle and cancellation of a dataflow pipeline.
/// </summary>
/// <remarks>
///     Creates and owns a <see cref="CancellationTokenSource" /> that is linked to an optional external token.
///     Provides <see cref="FlowContext" /> instances to pipeline stages and ensures proper
///     cancellation and resource cleanup via <see cref="IDisposable" /> and <see cref="IAsyncDisposable" />.
/// </remarks>
[PublicAPI]
public sealed class FlowSource : IDisposable, IAsyncDisposable
{
    private readonly Queue<object> _disposable = [];
    private readonly List<LazyScheduleTask> _tasks = [];
    private readonly CancellationTokenSource _cts = new();


    private Exception? _exception;
    private bool _disposed;

    /// <summary>
    ///     Registers a background task that the pipeline starts lazily on execution.
    /// </summary>
    /// <param name="handler">The factory that creates the background task.</param>
    /// <returns>A task that completes when the registered background task finishes.</returns>
    /// <remarks>
    ///     The task is not started immediately; it is run when <see cref="ExecuteAsync" /> is called.
    ///     The returned task surfaces the outcome (including exceptions), so a fault is never silently lost.
    /// </remarks>
    internal Task RegisterBackgroundProcess(Func<Task> handler)
    {
        ObjectDisposedException.ThrowIf(_disposed, typeof(FlowSource));
        
        var lazy = new LazyScheduleTask(handler);
        _tasks.Add(lazy);
        return lazy.Completion;
    }

    /// <summary>
    ///     Registers a disposable resource owned by the pipeline, released when the pipeline completes.
    /// </summary>
    /// <param name="disposable">
    ///     The resource to dispose on pipeline completion. Must implement <see cref="IDisposable" /> or
    ///     <see cref="IAsyncDisposable" />; otherwise the registration is ignored.
    /// </param>
    /// <remarks>
    ///     The resource is released in the owning <see cref="FlowSource" />'s cleanup phase, after the
    ///     registered background tasks have finished.
    /// </remarks>
    internal void RegisterDisposable(object disposable)
    {
        ObjectDisposedException.ThrowIf(_disposed, typeof(FlowSource));
        
        if (disposable is IAsyncDisposable or IDisposable)
            _disposable.Enqueue(disposable);
    }

    /// <summary>
    ///     Gets a <see cref="FlowContext" /> for use in pipeline stages.
    /// </summary>
    /// <value>A new flow context backed by this source's cancellation token source.</value>
    /// <exception cref="ObjectDisposedException">Thrown if the <see cref="FlowSource" /> has been disposed.</exception>
    internal FlowContext Context
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, typeof(FlowSource));
            return new FlowContext(this);
        }
    }
    
    /// <summary>
    ///     Gets the <see cref="CancellationToken" /> associated with this flow context.
    /// </summary>
    /// <value>
    ///     A valid token if the underlying source is not disposed;
    ///     a canceled token if the source has been disposed;
    ///     <see cref="CancellationToken.None" /> if no source was provided.
    /// </value>
    [PublicAPI]
    public CancellationToken Token => _cts.Token;
    
    /// <summary>
    ///     Signals cancellation to all stages in the pipeline that observe this context.
    /// </summary>
    /// <remarks>
    ///     Safe to call multiple times; subsequent calls are no-ops after the first cancellation.
    ///     Does not throw if the underlying source has already been disposed.
    /// </remarks>
    internal bool TryCancel(Exception ex)
    {
        if (Volatile.Read(ref _disposed))
            return false;

        try
        {
            if (Interlocked.CompareExchange(ref _exception, ex, null) is null)
            {
                _cts.Cancel();
                return true;
            }
        }
        catch (ObjectDisposedException)
        {
            // Nothing
        }
        
        return false;
    }

    /// <summary>
    ///     Asynchronously cancels the pipeline and releases all managed resources.
    /// </summary>
    /// <returns>A <see cref="ValueTask" /> representing the asynchronous dispose operation.</returns>
    /// <remarks>
    ///     Safe to call multiple times; subsequent calls are no-ops after the first disposal.
    ///     Uses <see cref="CancellationTokenSource.CancelAsync" /> for an async-friendly cancellation.
    /// </remarks>
    [PublicAPI]
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        await _cts.CancelAsync();
        _cts.Dispose();
        _disposed = true;
    }

    /// <summary>
    ///     Cancels the pipeline and releases all managed resources.
    /// </summary>
    /// <remarks>
    ///     Safe to call multiple times; subsequent calls are no-ops after the first disposal.
    /// </remarks>
    [PublicAPI]
    public void Dispose()
    {
        if (_disposed)
            return;

        _cts.Cancel();
        _cts.Dispose();
        _disposed = true;
    }

    /// <summary>
    ///     Starts all registered background tasks and waits for them, treating a fault as fatal.
    /// </summary>
    /// <returns>A task that completes when all registered tasks finish.</returns>
    /// <remarks>
    ///     All registered tasks are started; as soon as one fails or is canceled, the pipeline is canceled
    ///     and the aggregated exception is rethrown. Background tasks are expected to observe the flow token
    ///     so cancellation can stop them.
    /// </remarks>
    [PublicAPI]
    public async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, typeof(FlowSource));
        
        foreach (var lazy in _tasks)
            lazy.Run();

        List<Exception>? exception = null;

        // The external token cancels the flow: cancelling either stops the registered tasks. The wait
        // itself observes only the external token so a normal stop (via DisposeAsync → Cts.Cancel) does
        // not surface as an exception here.
        await using var ctr = cancellationToken.Register(() => _cts.Cancel());

        try
        {
            await foreach (var task in Task.WhenEach(_tasks.Select(static lazy => lazy.Completion)).WithCancellation(cancellationToken))
            {
                if (task is { IsCompletedSuccessfully: true })
                    continue;

                try
                {
                    await _cts.CancelAsync();
                }
                catch (Exception ex0)
                {
                    exception ??= [];
                    exception.Add(ex0);
                }

                // A canceled task is a normal pipeline stop (e.g. DisposeAsync), not a fault — it is
                // not surfaced. Only real faults (task.Exception) are aggregated and rethrown.
                if (task.Exception is null)
                    continue;
                
                exception ??= [];
                exception.AddRange(task.Exception.InnerExceptions);
            }
        }
        finally
        {
            try
            {
                await _cts.CancelAsync();
            }
            catch (Exception ex1)
            {
                exception ??= [];
                exception.Add(ex1);
            }

            while (_disposable.TryDequeue(out var disposable))
            {
                try
                {
                    switch (disposable)
                    {
                        case IAsyncDisposable d:
                            await d.DisposeAsync();
                            break;
                        case IDisposable d:
                            d.Dispose();
                            break;
                    }
                }
                catch (Exception ex2) when (ex2 is not NotSupportedException)
                {
                    exception ??= [];
                    exception.Add(ex2);
                }
            }
            
            _tasks.Clear();
        }
        
        _disposed = true;

        if (Volatile.Read(ref _exception) is { } ex3)
        {
            exception ??= [];
            exception.Add(ex3);
        }
        
        if (exception is not null && exception.Count > 0)
            throw new AggregateException(exception);
    }
}