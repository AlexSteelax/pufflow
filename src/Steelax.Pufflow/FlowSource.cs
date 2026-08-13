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
    private readonly CancellationTokenSource _cts;
    private bool _disposed;

    private FlowSource(CancellationTokenSource cts)
    {
        _cts = cts;
    }

    /// <summary>
    ///     Initializes a new <see cref="FlowSource" /> linked to an external cancellation token.
    /// </summary>
    /// <param name="cancellationToken">
    ///     An external token that can cancel the pipeline.
    ///     A linked token source is created so that disposing <see cref="FlowSource" />
    ///     does not affect the original token source.
    /// </param>
    [PublicAPI]
    public FlowSource(CancellationToken cancellationToken)
        : this(CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
    {
    }

    /// <summary>
    ///     Initializes a new <see cref="FlowSource" /> with its own cancellation token source.
    /// </summary>
    [PublicAPI]
    public FlowSource() : this(new CancellationTokenSource())
    {
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
            return new FlowContext(_cts);
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
}