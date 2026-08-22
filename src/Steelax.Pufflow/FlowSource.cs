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
    private readonly List<object> _disposable = [];
    private readonly List<LazyScheduleTask> _tasks = [];
    internal readonly CancellationTokenSource Cts = new();
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
    [PublicAPI]
    public Task RegisterBackground(Func<Task> handler)
    {
        var lazy = new LazyScheduleTask(handler);
        _tasks.Add(lazy);
        return lazy.ExecuteTask;
    }

    internal void RegisterDisposable(object disposable)
    {
        if (disposable is IAsyncDisposable or IDisposable)
            _disposable.Add(disposable);
    }

    /// <summary>
    ///     Gets a <see cref="FlowContext" /> for use in pipeline stages.
    /// </summary>
    /// <value>A new flow context backed by this source's cancellation token source.</value>
    /// <exception cref="ObjectDisposedException">Thrown if the <see cref="FlowSource" /> has been disposed.</exception>
    [PublicAPI]
    public FlowContext Context
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, typeof(FlowSource));
            return new FlowContext(this);
        }
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

        await Cts.CancelAsync();
        Cts.Dispose();
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

        Cts.Cancel();
        Cts.Dispose();
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
        var tasks = _tasks.ToArray();
        foreach (var lazy in tasks)
            lazy.Run();

        List<Exception>? exception = null;

        // The external token cancels the flow: cancelling either stops the registered tasks. The wait
        // itself observes only the external token so a normal stop (via DisposeAsync → Cts.Cancel) does
        // not surface as an exception here.
        await using var ctr = cancellationToken.Register(() => Cts.Cancel());

        try
        {
            await foreach (var task in Task.WhenEach(tasks.Select(static lazy => lazy.ExecuteTask)).WithCancellation(cancellationToken))
            {
                if (task is { IsCompletedSuccessfully: false })
                {
                    await Cts.CancelAsync();

                    // A canceled task is a normal pipeline stop (e.g. DisposeAsync), not a fault — it is
                    // not surfaced. Only real faults (task.Exception) are aggregated and rethrown.
                    if (task.Exception is not null)
                    {
                        exception ??= [];
                        exception.AddRange(task.Exception.InnerExceptions);
                    }
                }
            }
        }
        finally
        {
            await Cts.CancelAsync();
            
            foreach (var disposable in _disposable)
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
                catch (Exception ex) when (ex is not NotSupportedException)
                {
                    exception ??= [];
                    exception.Add(ex);
                }
            }
            
            _disposable.Clear();
            _tasks.Clear();
        }
        
        if (exception is not null && exception.Count > 0)
            throw new AggregateException(exception);
    }
}