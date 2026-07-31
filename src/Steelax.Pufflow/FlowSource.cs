namespace Steelax.Pufflow;

[PublicAPI]
public sealed class FlowSource : IDisposable, IAsyncDisposable
{
    private readonly CancellationTokenSource _cts;
    private bool _disposed;
    
    private FlowSource(CancellationTokenSource cts) => _cts = cts;
    
    [PublicAPI]
    public FlowSource(CancellationToken cancellationToken) => _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
    
    [PublicAPI]
    public FlowSource() => _cts = new CancellationTokenSource();

    [PublicAPI]
    public FlowContext Context
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, typeof(FlowSource));
            return new FlowContext(_cts);
        }
    }
    
    [PublicAPI]
    public void Dispose()
    {
        if (_disposed)
            return;
        
        _cts.Cancel();
        _cts.Dispose();
        _disposed = true;
    }
    
    [PublicAPI]
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;
        
        await _cts.CancelAsync();
        _cts.Dispose();
        _disposed = true;
    }
}