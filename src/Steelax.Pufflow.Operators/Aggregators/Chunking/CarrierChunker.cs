using System.Buffers;
using System.Runtime.CompilerServices;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Aggregators.Chunking;

/// <summary>
///     A pool-backed chunk builder that produces <see cref="Chunk{T}" /> instances.
/// </summary>
/// <typeparam name="T">The type of the accumulated elements.</typeparam>
/// <remarks>
///     Rents buffers from <see cref="ArrayPool{T}.Shared" /> and returns them when chunks are completed
///     or the builder is disposed. This type is not thread-safe.
/// </remarks>
[PublicAPI]
public sealed class CarrierChunker<T> : IChunkBuilder<Carrier<T>, Carrier<Chunk<T>>>
{
    private static readonly ArrayPool<T> Pool = ArrayPool<T>.Shared;

    private readonly int _minimumSize;
    private readonly ChunkCapacityStrategy _strategy;
    private T[]? _buffer;

    private Watermark _watermark = Watermark.Nothing();
    
    private int _capacity;
    private int _count;

    /// <summary>
    ///     Initializes a new instance of the <see cref="Chunker{T}" /> class.
    /// </summary>
    /// <param name="minimumSize"></param>
    /// <param name="strategy">
    ///     The strategy that determines how the rented buffer sizes a chunk.
    /// </param>
    public CarrierChunker(int minimumSize, ChunkCapacityStrategy strategy = ChunkCapacityStrategy.Exact)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(minimumSize);
        
        _minimumSize = minimumSize;
        _strategy = strategy;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Rent()
    {
        if (_buffer is not null)
            return;

        _buffer = Pool.Rent(_minimumSize);
        _capacity = _strategy == ChunkCapacityStrategy.Exact ? _minimumSize : _buffer.Length;
        _count = 0;
    }

    /// <inheritdoc />
    public void Add(Carrier<T> item)
    {
        Rent();
        
        if (item.HasValue)
            _buffer![_count++] = item.Value;
        
        if (item.HasWatermark && item.Watermark > _watermark)
            _watermark = item.Watermark;
    }

    /// <inheritdoc />
    public bool TryGet(out Carrier<Chunk<T>> chunk)
    {
        var buffer = _buffer;

        if (buffer is null || _count == 0 && _watermark.IsNothing)
        {
            chunk = default;
            return false;
        }

        if (_count == 0)
        {
            chunk = new Carrier<Chunk<T>>(_watermark);
            _watermark = Watermark.Nothing();
            return true;
        }

        chunk = new Carrier<Chunk<T>>(new Chunk<T>(buffer, _count), _watermark);
        
        _watermark = Watermark.Nothing();
        _buffer = null;
        _count = 0;

        return true;
    }

    /// <inheritdoc />
    public bool IsEmpty => _count == 0 && _watermark.IsNothing;

    /// <inheritdoc />
    public bool IsFull => _count == _capacity && _capacity != 0;

    /// <summary>
    ///     Returns the current buffer to the pool, if it has not been completed.
    /// </summary>
    public void Dispose()
    {
        if (_buffer is null)
            return;
        
        Pool.Return(_buffer, true);
        _buffer = null;
        _watermark = Watermark.Nothing();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void Return(T[] buffer)
    {
        Pool.Return(buffer, true);
    }
}