using System.Runtime.CompilerServices;

namespace Steelax.Pufflow;

[PublicAPI]
public readonly struct FlowContext
{
    private readonly CancellationTokenSource? _cts;

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
    
    [PublicAPI]
    public static implicit operator CancellationToken(FlowContext context) => context.Token;

    internal FlowContext(CancellationTokenSource cancellationTokenSource) => _cts = cancellationTokenSource;
    
    [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_disposed")]
    private static extern ref bool IsDisposed(CancellationTokenSource target);
}