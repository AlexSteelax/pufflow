using System.Runtime.CompilerServices;
using Steelax.Pufflow.Operators.Common;
using static Steelax.Toolkit.HighPerformance.AdvanceBitOperations;

namespace Steelax.Pufflow.Operators.Internal;


internal struct WatermarkBarrier
{
    private const int MaxSize = 32;
    
    private readonly int _count;
    private WatermarkCollection _collection;
    
    // флаги установленных меток в текущем раунде
    private uint _round;
    // флаги завершенных источников
    private uint _completed;

    [PublicAPI]
    public WatermarkBarrier(int count)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan(count, MaxSize);
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        
        // по умолчанию 0 для первых count, остальные должны быть всегда равны 1
        // устанавливаем первые биты в заданном кол-ве в 1 и инвертируем получая необходимое значение
        _round = ~OfFirstBits(count);
        _completed = ~OfFirstBits(count);
        
        _count = count;
        
        _collection[..].Fill(Watermark.Nothing);
    }

    [PublicAPI]
    public void Set(int index, Watermark watermark)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, _count);
        ArgumentOutOfRangeException.ThrowIfEqual(_completed, uint.MaxValue);
        
        // Если значение не является значащим, то не учитываем его в раунде (оптимизация)
        if (watermark.IsNothing)
            return;
        
        ref var current = ref _collection[index];

        Debug.Assert(watermark >= current, "Next watermark must be greater or equal of current.");
        
        // Если значение не изменилось (или меньше - некорректное состояние), то не учитываем его в раунде (оптимизация)
        if (watermark <= current)
            return;
        
        current = watermark;
        
        SetBit(ref _round, index);
    }

    [PublicAPI]
    public void Complete(int index)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, _count);
        ArgumentOutOfRangeException.ThrowIfEqual(_completed, uint.MaxValue);
        
        // Если флаг завершенности уже установлен, то выходим из метода
        if (!TrySetBit(ref _completed, index))
            return;
        
        // Исправление текущего round
        // В дальнейшем сброс раунда это учитывает через completed-маску
        SetBit(ref _round, index);
    }
    
    [PublicAPI]
    public bool IsCompleted => HasAllBits(_completed);
    
    [PublicAPI]
    public bool HasCompleted(int index) => IsBitSet(_completed, index);

    [PublicAPI]
    public bool NextRound(out Watermark watermark)
    {
        if (HasAllBits(_round))
        {
            // Если все источники завершены, то поиск минимального среди незавершенных невозможен, вместо этого ищем максимум среди всех
            // Поиск максимума нужен единожды, поэтому стоимость не имеет значения
            if (HasAllBits(_completed))
            {
                // Сброс раунда
                _round = 0;
                watermark = FindMaxWatermark(_collection[.._count]);
            }
            else
            {
                // Сброс раунда (устанавливаются только активные источники)
                _round = _completed;
                watermark = FindMinWatermark(_collection[.._count], ~_completed);
            }
            
            return true;
        }

        watermark = default;
        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Watermark FindMaxWatermark(scoped in ReadOnlySpan<long> data)
    {
        if (data.Length == 0)
            return Watermark.Nothing;

        var max = data[0];
        
        for (var i = 1; i < data.Length; i++)
            max = Math.Max(max, data[i]);

        return Watermark.From(max);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)] // Встраивает код прямо в место вызова
    private static Watermark FindMinWatermark(scoped in ReadOnlySpan<long> data, uint activeMask)
    {
        // Если все элементы исключены (завершены)
        if (!TryPopBit(ref activeMask, out var index))
            return Watermark.Nothing;

        var min = data[index];

        while (TryPopBit(ref activeMask, out index))
            min = Math.Min(min, data[index]);

        return Watermark.From(min);
    }
    
    [InlineArray(MaxSize)]
    private struct WatermarkCollection
    {
        private long _element0;
    }
}