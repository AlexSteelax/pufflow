using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Operators.Internal;
using Steelax.Toolkit.HighPerformance;
using Steelax.Toolkit.HighPerformance.Concurrency.Primitives;

namespace Steelax.Pufflow.Operators.Aggregators;

[Flow]
internal sealed partial class PeriodicRunnerProcessor<T> : IAsyncConsumator<Carrier<T>>, IDisposable, IAsyncDisposable
{
    private const int SourceReadyLabel = 0;
    private const int IntervalCompleteLabel = 1;

    private readonly Action _handler;
    private readonly WatchTimer<FanInSlim> _runnerTimer;
    private readonly FanInSlim _fanInSlim;
    
    private ManualConsumator<Carrier<T>> _source = null!;

    private Watermark _lastWatermark = Watermark.Nothing;

    public PeriodicRunnerProcessor(Action handler, TimeSpan period, TimeProvider? timeProvider = null)
    {
        _handler = handler;
        _fanInSlim = new FanInSlim();
        _runnerTimer = new WatchTimer<FanInSlim>(static fanInSlim => fanInSlim.Signal(IntervalCompleteLabel), _fanInSlim, period, timeProvider);
    }
    
    public void Fuse(IAsyncConsumator<Carrier<T>> source, out IAsyncConsumator<Carrier<T>> target, FlowContext context)
    {
        _source = ManualConsumator<Carrier<T>>.Create(source);
        target = this;
        context.OnCancelled(_ => _fanInSlim.Complete());
        
        _source.OnReady += _fanInSlim.GetSignalCallback(SourceReadyLabel).Handler;

        // The recurrence kickoff: the timer is created stopped and must be started once the processor is
        // wired into a live pipeline.
        _runnerTimer.Start();
    }

    private bool HandleExecute(out Carrier<T> carrier)
    {
        // Вызываем обработчик
        _handler.Invoke();

        // Если watermark  с прошлых этапов прошел, то отдаем его в поток
        // Т.к. watermark отделен от данных, то это обеспечивает естественный heartbeat.
        if (!_lastWatermark.IsNothing)
        {
            carrier = new Carrier<T>(_lastWatermark);
            _lastWatermark = Watermark.Nothing;
            return true;
        }

        carrier = default;
        return false;
    }

    private bool TrySourceRead(out Carrier<T> value)
    {
        // Фантомные Carrier (без value и watermark) нет смысла продвигать, т.к. они не толкают прогресс
        while (_source.TryGet(out var carrier))
        {
            Debug.Assert(carrier.Watermark >= _lastWatermark);
            
            _source.Ack();
            
            if (carrier.HasWatermark)
                _lastWatermark = carrier.Watermark;

            if (carrier.HasValue)
            {
                value = new Carrier<T>(carrier.Value, Watermark.Nothing);
                return true;
            }
        }
        
        value = default;
        return false;
    }

    public bool TryRead(out Carrier<T> value)
    {
        var state = _fanInSlim.Take();

        if (_fanInSlim.IsCompleted)
        {
            value = default;
            return false;
        }
        
        // Проверяем срабатывание таймера
        if (AdvanceBitOperations.IsBitSet(state, IntervalCompleteLabel) && HandleExecute(out value))
            return true;
        
        if (TrySourceRead(out value))
            return true;

        if (_source.IsCompleted)
        {
            _fanInSlim.Complete();
            
            // Финализируем обработчиком
            if (HandleExecute(out value))
                return true;
        }
        
        value = default;
        return false;
    }

    public bool IsCompleted => _fanInSlim.IsCompleted;
    
    public ValueTask<bool> WaitToReadAsync()
    {
        var state = _fanInSlim.Peek();

        return AdvanceBitOperations.HasAnyBits(state)
            ? ValueTask.FromResult(true)
            : _fanInSlim.WaitAsync();
    }

    public void Dispose()
    {
        _runnerTimer.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        await _runnerTimer.DisposeAsync();
    }
}