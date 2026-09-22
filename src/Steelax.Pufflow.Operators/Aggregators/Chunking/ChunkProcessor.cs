using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Steelax.Pufflow.Operators.Internal;
using Steelax.Toolkit.HighPerformance.Concurrency.Primitives;

namespace Steelax.Pufflow.Operators.Aggregators.Chunking;

[Flow]
internal sealed partial class ChunkProcessor<T, TChunk> : IAsyncConsumator<TChunk>, IAsyncDisposable
{
    private readonly IChunkBuilder<T, TChunk> _chunker;
    private readonly TimeSpan _linger;

    private readonly CompleteSignal _signal;
    private readonly WatchTimer<ChunkProcessor<T, TChunk>> _timer;

    private ManualConsumator<T> _source = null!;
    private CancellationToken _cancellationToken;
    private bool _ready;

    public ChunkProcessor(IChunkBuilder<T, TChunk> chunker, TimeSpan linger, TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(chunker);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(linger, TimeSpan.Zero);

        _chunker = chunker;
        _linger = linger;
        _signal = new CompleteSignal();
        _timer = new WatchTimer<ChunkProcessor<T, TChunk>>(static self => self.FireLingerSignal(), this, timeProvider);
    }
    
    public void Fuse(IAsyncConsumator<T> source, out IAsyncConsumator<TChunk> target, FlowContext context)
    {
        _source = ManualConsumator<T>.Create(source);
        _source.OnReady += _signal.Signal;
        _cancellationToken = context.Token;
        target = this;
        
        context.RegisterDisposable(this);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryFlush([MaybeNullWhen(false)] out TChunk chunk)
    {
        var ready = Volatile.Read(ref _ready);
        
        if (ready || _source.IsCompleted)
            StopLinger();

        if (_chunker.IsFull || _source.IsCompleted || ready)
            return _chunker.TryGet(out chunk);
        
        chunk = default;
        return false;
    }

    public bool TryRead([MaybeNullWhen(false)] out TChunk chunk)
    {
        if (IsCompleted)
        {
            _signal.Complete();
            chunk = default;
            // exception fallback if exists
            return _source.TryGet(out _);
        }
        
        if (TryFlush(out chunk))
            return true;
        
        while (!_cancellationToken.IsCancellationRequested)
        {
            if (_source.TryGet(out var item))
            {
                var empty = _chunker.IsEmpty;
                
                Debug.Assert(!_chunker.IsFull);
                _chunker.Add(item);
                
                if (empty && !_chunker.IsEmpty)
                    StartLinger();
                
                _source.Ack();
            }
            else
            {
                chunk = default;
                return false;
            }
            
            if (TryFlush(out chunk))
                return true;
        }
        
        chunk = default;
        return false;
    }

    public bool IsCompleted => _source.IsCompleted && _chunker.IsEmpty;

    public ValueTask<bool> WaitToReadAsync()
    {
        if (IsCompleted)
            return ValueTask.FromResult(false);
        
        return _signal.WaitAsync();
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void FireLingerSignal()
    {
        Volatile.Write(ref _ready, true);
        _signal.Signal();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void StopLinger()
    {
        _timer.Stop();
        Volatile.Write(ref _ready, false);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void StartLinger()
    {
        _timer.Start(_linger);
    }

    public async ValueTask DisposeAsync()
    {
        await _timer.DisposeAsync();
        _chunker.Dispose();
    }
}