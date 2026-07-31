using System.Runtime.CompilerServices;

namespace Steelax.Pufflow;

/// <summary>
/// Provides cancellation support for a dataflow pipeline stage.
/// </summary>
/// <remarks>
/// Wraps a <see cref="CancellationTokenSource"/> and exposes the token via <see cref="Token"/>.
/// Implicitly converts to <see cref="CancellationToken"/> for seamless integration with async APIs.
/// Uses an unsafe accessor to check <c>CancellationTokenSource._disposed</c> field
/// to avoid <see cref="ObjectDisposedException"/> in thread-safe scenarios.
/// This struct is intended to be passed through pipeline stages to propagate cancellation signals.
/// </remarks>
[PublicAPI]
public readonly struct FlowContext
{
    private readonly CancellationTokenSource? _cts;

    /// <summary>
    /// Gets the <see cref="CancellationToken"/> associated with this flow context.
    /// </summary>
    /// <value>
    /// A valid token if the underlying source is not disposed;
    /// a canceled token if the source has been disposed;
    /// <see cref="CancellationToken.None"/> if no source was provided.
    /// </value>
    [PublicAPI]
    public CancellationToken Token
    {
        get
        {
            if (_cts is null)
                return CancellationToken.None;

            if (IsDisposed(_cts))
                return new CancellationToken(canceled: true);

            try
            {
                return _cts.Token;
            }
            catch (ObjectDisposedException)
            {
                return new CancellationToken(canceled: true);
            }
        }
    }

    /// <summary>
    /// Signals cancellation to all stages in the pipeline that observe this context.
    /// </summary>
    /// <remarks>
    /// Safe to call multiple times; subsequent calls are no-ops after the first cancellation.
    /// Does not throw if the underlying source has already been disposed.
    /// </remarks>
    [PublicAPI]
    public void Cancel()
    {
        if (_cts is null || IsDisposed(_cts))
            return;

        try
        {
            _cts.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // Nothing
        }
    }

    /// <summary>
    /// Implicitly converts a <see cref="FlowContext"/> to a <see cref="CancellationToken"/>.
    /// </summary>
    /// <param name="context">The flow context to convert.</param>
    [PublicAPI]
    public static implicit operator CancellationToken(FlowContext context) => context.Token;

    /// <summary>
    /// Initializes a new flow context backed by the specified cancellation token source.
    /// </summary>
    /// <param name="cancellationTokenSource">The source that provides the cancellation token. May be null.</param>
    internal FlowContext(CancellationTokenSource cancellationTokenSource) => _cts = cancellationTokenSource;

    /// <summary>
    /// Reads the <c>_disposed</c> field of a <see cref="CancellationTokenSource"/>
    /// via an unsafe accessor to avoid <see cref="ObjectDisposedException"/>.
    /// </summary>
    /// <param name="target">The cancellation token source to inspect.</param>
    /// <returns><see langword="true"/> if the source has been disposed; otherwise, <see langword="false"/>.</returns>
    [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_disposed")]
    private static extern ref bool IsDisposed(CancellationTokenSource target);
}