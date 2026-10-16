using System.Runtime.CompilerServices;

namespace Steelax.Pufflow;

/// <summary>
///     Provides cancellation support for a dataflow pipeline stage.
/// </summary>
/// <remarks>
///     Wraps the owning <see cref="FlowSource" /> and exposes the flow token via <see cref="Token" />.
///     Implicitly converts to <see cref="System.Threading.CancellationToken" /> for seamless integration with async APIs.
///     A disposed source is reported as an already-canceled token rather than an
///     <see cref="ObjectDisposedException" />, keeping stage reads safe after the pipeline completes.
///     This struct is intended to be passed through pipeline stages to propagate cancellation signals.
/// </remarks>
[PublicAPI]
public readonly struct FlowContext
{
    private readonly FlowSource? _source;

    /// <inheritdoc cref="FlowSource.Token"/>
    [PublicAPI]
    public CancellationToken Token
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            if (_source is null)
                return CancellationToken.None;
            
            try
            {
                return _source.Token;
            }
            catch (ObjectDisposedException)
            {
                Trace.WriteLine($"{nameof(FlowContext)} has already been disposed of.");
                return new CancellationToken(true);
            }
        }
    }

    /// <inheritdoc cref="FlowSource.TryCancel"/>
    [PublicAPI]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryCancel(Exception ex) => _source?.TryCancel(ex) ?? false;

    /// <inheritdoc cref="FlowSource.RegisterBackgroundProcess"/>
    [PublicAPI]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task RegisterBackgroundProcess(Func<FlowContext, Task> handler)
    {
        if (_source is null)
            return handler.Invoke(default);
        
        var source = _source;

        return _source.RegisterBackgroundProcess(() => handler.Invoke(source.Context));
    }
    
    /// <inheritdoc cref="FlowSource.RegisterBackgroundProcess"/>
    [PublicAPI]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task RegisterBackgroundProcess<TArgs>(Func<FlowContext, TArgs, Task> handler, TArgs args)
    {
        if (_source is null)
            return handler.Invoke(default, args);
        
        var source = _source;

        return _source.RegisterBackgroundProcess(() => handler.Invoke(source.Context, args));
    }

    /// <summary>
    ///     Registers a callback that runs when the pipeline is canceled.
    /// </summary>
    /// <param name="handler">
    ///     The callback to invoke, receiving the <see cref="System.Threading.CancellationToken" /> that was canceled.
    /// </param>
    /// <remarks>
    ///     The registration is owned by the pipeline and released automatically when it completes.
    /// </remarks>
    [PublicAPI]
    public void OnCancelled(Action<CancellationToken> handler)
    {
        if (_source is null)
            return;
        
        var registration = _source.Token.Register(static (handler, token) => ((Action<CancellationToken>)handler!).Invoke(token), handler);

        try
        {
            _source.RegisterDisposable(registration);
        }
        catch
        {
            registration.Dispose();
            throw;
        }
    }

    /// <summary>
    ///     Implicitly converts a <see cref="FlowContext" /> to a <see cref="System.Threading.CancellationToken" />.
    /// </summary>
    /// <param name="context">The flow context to convert.</param>
    [PublicAPI]
    public static implicit operator CancellationToken(FlowContext context) => context.Token;

    /// <summary>
    ///     Initializes a new flow context backed by the specified flow source.
    /// </summary>
    /// <param name="source">The flow source that owns cancellation and the registered background tasks.</param>
    internal FlowContext(FlowSource source) => _source = source;
}