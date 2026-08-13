using System.Buffers;
using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

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
public sealed class Chunker<T> : IChunkBuilder<T, Chunk<T>>, IDisposable
{
    private static readonly ArrayPool<T> Pool = ArrayPool<T>.Shared;

    private readonly ChunkCapacityStrategy _strategy;
    private T[]? _buffer;
    private int _capacity;
    private int _count;

    /// <summary>
    ///     Initializes a new instance of the <see cref="Chunker{T}" /> class.
    /// </summary>
    /// <param name="strategy">
    ///     The strategy that determines how the rented buffer sizes a chunk.
    /// </param>
    public Chunker(ChunkCapacityStrategy strategy = ChunkCapacityStrategy.Exact)
    {
        _strategy = strategy;
    }

    /// <inheritdoc />
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Rent(int minimumSize)
    {
        if (_buffer is not null)
            throw new InvalidOperationException("The current chunk must be completed before renting a new buffer.");

        _buffer = Pool.Rent(minimumSize);
        _capacity = _strategy == ChunkCapacityStrategy.Exact ? minimumSize : _buffer.Length;
        _count = 0;
    }

    /// <inheritdoc />
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryAdd(T item)
    {
        if (_count == _capacity)
            return false;

        _buffer![_count++] = item;
        return true;
    }

    /// <inheritdoc />
    public bool IsEmpty => _count == 0;

    /// <inheritdoc />
    public bool IsCompleted => _count == _capacity;

    /// <inheritdoc />
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryComplete(out Chunk<T> chunk)
    {
        var buffer = _buffer;

        if (buffer is null || _count == 0)
        {
            chunk = default;
            return false;
        }

        chunk = new Chunk<T>(buffer, _count);
        
        _buffer = null;
        _count = 0;

        return true;
    }

    /// <summary>
    ///     Returns the current buffer to the pool, if it has not been completed.
    /// </summary>
    public void Dispose()
    {
        if (_buffer is null)
            return;
        
        Pool.Return(_buffer, true);
        _buffer = null;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void Return(T[] buffer)
    {
        Pool.Return(buffer, true);
    }
}