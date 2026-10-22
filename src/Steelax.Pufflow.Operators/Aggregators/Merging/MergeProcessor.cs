using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Operators.Internal;
using Steelax.Toolkit.HighPerformance.Concurrency.Primitives;
using static Steelax.Toolkit.HighPerformance.AdvanceBitOperations;

namespace Steelax.Pufflow.Operators.Aggregators.Merging;

[Flow]
internal sealed partial class MergeProcessor<T> : IAsyncConsumator<Carrier<T>>
{
    private readonly FanInSlim _fanIn;
    private readonly ManualConsumator<Carrier<T>>[] _sources;
    
    private readonly int _count;

    private WatermarkBarrier _barrier;
    private uint _pending;
    private uint _ready;
    
    private int _globalIdx = -1;
    
    public MergeProcessor(IAsyncConsumator<Carrier<T>>[] sources)
    {
        // Поддерживаем только до 32 источников для слияния. Этого должно быть более чем достаточно.
        ArgumentOutOfRangeException.ThrowIfGreaterThan(sources.Length, 32, nameof(sources));

        _fanIn = new FanInSlim();
        _sources = new ManualConsumator<Carrier<T>>[sources.Length];
        
        _count = sources.Length;
        _barrier = new WatermarkBarrier(sources.Length);
        
        _ready = 0;
        _pending = 0;
        
        for (var i = 0; i < sources.Length; i++)
        {
            var consumator = _sources[i] = ManualConsumator<Carrier<T>>.Create(sources[i]);
            consumator.OnReady += _fanIn.GetSignalCallback(i).Handler;
        }
    }
    
    public void Fuse(out IAsyncConsumator<Carrier<T>> target, FlowContext context)
    {
        target = this;
        context.OnCancelled(_ => _fanIn.Complete());
    }
    
    public bool TryRead(out Carrier<T> value)
    {
        value = default;
        
        if (_fanIn.IsCompleted)
            return false;
        
        var ret =
            // Проверка раунда, чтобы своевременно выдать результат, фактически генерируя heartbeat-watermark
            TryRoundGet(ref value) ||
            // Довыгружаем готовое перед тем как перейти к активному опросу
            TryPendingGet(ref value) ||
            // Если довыгружать нечего, то переходим к round-robin опросу
            TrySourceGet(ref value) ||
            // Т.к. источники конкурентные, то прежде чем перейти к завершающему этапу, делаем повторную проверку на готовность источников
            TryPendingGet(ref value) ||
            // Повторная проверка раунда в конце: выгрузка данных могла завершить источники и добить раунд,
            // поэтому финальный маркер (максимум по завершённым) должен быть выдан здесь, до закрытия fan-in
            TryRoundGet(ref value);

        if (_barrier.IsCompleted)
            _fanIn.Complete();
        
        return ret;
    }
    
    private bool TryRoundGet(scoped ref Carrier<T> value)
    {
        if (!_barrier.NextRound(out var watermark) || watermark.IsNothing)
            return false;
        
        value = new Carrier<T>(watermark);
        return true;
    }
    
    private bool TryPendingGet(scoped ref Carrier<T> value)
    {
        if (!HasAnyBits(_ready))
            _ready = _fanIn.Take();
        
        while (TryPopBit(ref _ready, out var idx))
        {
            ref var source = ref _sources[idx];
            
            if (!source.TryGet(out var carrier))
            {
                // Устанавливаем либо флаг завершенности, либо ожидания
                if (source.IsCompleted)
                    _barrier.Complete(idx);
                else
                    SetBit(ref _pending, idx);

                continue;
            }

            UnsetBit(ref _pending, idx);
            
            value = carrier.WithWatermark(Watermark.Nothing);
            _barrier.Set(idx, carrier.Watermark);
            source.Ack();

            return true;
        }
        
        return false;
    }

    private bool TrySourceGet(scoped ref Carrier<T> value)
    {
        if (++_globalIdx >= _count)
        {
            _globalIdx = 0;
        }
        
        for (; _globalIdx < _count; _globalIdx++)
        {
            // Пропускаем завершенные или поставленные на ожидание (ленивая постановка)
            if (IsBitSet(_pending, _globalIdx) || _barrier.HasCompleted(_globalIdx))
                continue;
            
            ref var source = ref _sources[_globalIdx];

            if (!source.TryGet(out var carrier))
            {
                // Устанавливаем либо флаг завершенности, либо ожидания
                if (source.IsCompleted)
                    _barrier.Complete(_globalIdx);
                else
                    SetBit(ref _pending, _globalIdx);

                continue;
            }
            
            UnsetBit(ref _pending, _globalIdx);
            
            value = carrier.WithWatermark(Watermark.Nothing);
            _barrier.Set(_globalIdx, carrier.Watermark);
            source.Ack();

            // Выставляем обратно сигнал готовности, т.к. текущие данные прочитали, а значит потенциально на следующем витке есть данные
            SetBit(ref _ready, _globalIdx);
            
            return true;
        }
        
        return false;
    }

    public bool IsCompleted => _fanIn.IsCompleted;

    public ValueTask<bool> WaitToReadAsync()
    {
        return HasAnyBits(_ready) ? ValueTask.FromResult(true) : _fanIn.WaitAsync();
    }
}