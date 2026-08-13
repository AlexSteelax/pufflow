using System.Runtime.CompilerServices;

namespace Steelax.Pufflow;

/// <summary>
///     Provides cancellation support for a dataflow pipeline stage.
/// </summary>
/// <remarks>
///     Wraps a <see cref="CancellationTokenSource" /> and exposes the token via <see cref="Token" />.
///     Implicitly converts to <see cref="CancellationToken" /> for seamless integration with async APIs.
///     Uses an unsafe accessor to check <c>CancellationTokenSource._disposed</c> field
///     to avoid <see cref="ObjectDisposedException" /> in thread-safe scenarios.
///     This struct is intended to be passed through pipeline stages to propagate cancellation signals.
/// </remarks>
[PublicAPI]
public readonly struct FlowContext
{
    private readonly FlowSource? _source;

    /// <summary>
    ///     Gets the <see cref="CancellationToken" /> associated with this flow context.
    /// </summary>
    /// <value>
    ///     A valid token if the underlying source is not disposed;
    ///     a canceled token if the source has been disposed;
    ///     <see cref="CancellationToken.None" /> if no source was provided.
    /// </value>
    [PublicAPI]
    public CancellationToken Token
    {
        get
        {
            var cts = _source?.Cts;

            if (cts is null)
                return CancellationToken.None;

            if (IsDisposed(cts))
                return new CancellationToken(true);

            try
            {
                return cts.Token;
            }
            catch (ObjectDisposedException)
            {
                return new CancellationToken(true);
            }
        }
    }

    /// <summary>
    ///     Signals cancellation to all stages in the pipeline that observe this context.
    /// </summary>
    /// <remarks>
    ///     Safe to call multiple times; subsequent calls are no-ops after the first cancellation.
    ///     Does not throw if the underlying source has already been disposed.
    /// </remarks>
    [PublicAPI]
    public void Cancel()
    {
        var cts = _source?.Cts;

        if (cts is null || IsDisposed(cts))
            return;

        try
        {
            cts.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // Nothing
        }
    }

    /// <summary>
    ///     Registers a background task for the pipeline, or runs it immediately when no owning
    ///     <see cref="FlowSource" /> is attached.
    /// </summary>
    /// <param name="handler">The factory that creates the background task.</param>
    /// <returns>A task that completes when the registered background task finishes.</returns>
    /// <remarks>
    ///     The task is started lazily by the owning <see cref="FlowSource" /> (see
    ///     <c>FlowSource.ExecuteAsync</c>); the returned task surfaces its outcome (including exceptions).
    ///     When no source is attached, the handler is invoked immediately as a fallback.
    /// </remarks>
    [PublicAPI]
    public Task RegisterBackground(Func<Task> handler)
    {
        return _source?.RegisterBackground(handler) ?? handler.Invoke();
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
    [PublicAPI]
    public void RegisterDisposable(object disposable)
    {
        _source?.RegisterDisposable(disposable);
    }

    /// <summary>
    ///     Implicitly converts a <see cref="FlowContext" /> to a <see cref="CancellationToken" />.
    /// </summary>
    /// <param name="context">The flow context to convert.</param>
    [PublicAPI]
    public static implicit operator CancellationToken(FlowContext context)
    {
        return context.Token;
    }

    /// <summary>
    ///     Initializes a new flow context backed by the specified flow source.
    /// </summary>
    /// <param name="source">The flow source that owns cancellation and the registered background tasks.</param>
    internal FlowContext(FlowSource source)
    {
        _source = source;
    }

    /// <summary>
    ///     Reads the <c>_disposed</c> field of a <see cref="CancellationTokenSource" />
    ///     via an unsafe accessor to avoid <see cref="ObjectDisposedException" />.
    /// </summary>
    /// <param name="target">The cancellation token source to inspect.</param>
    /// <returns><see langword="true" /> if the source has been disposed; otherwise, <see langword="false" />.</returns>
    [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_disposed")]
    private static extern ref bool IsDisposed(CancellationTokenSource target);
}