using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Steelax.Pufflow.Operators;

/// <summary>
///     Defines how a rented buffer is used to size a chunk.
/// </summary>
[PublicAPI]
public enum ChunkCapacityStrategy
{
    /// <summary>The chunk holds at most the requested size; extra rented capacity is unused.</summary>
    Exact,

    /// <summary>The chunk fills the entire rented buffer; the requested size is the minimum rent.</summary>
    Fill
}

/// <summary>
///     Provides a reusable builder that accumulates elements into chunks.
/// </summary>
/// <typeparam name="T">The type of the accumulated elements.</typeparam>
/// <typeparam name="TChunk">The type of the produced chunk.</typeparam>
[PublicAPI]
public interface IChunkBuilder<in T, TChunk>
{
    /// <summary>Gets a value indicating whether the current chunk contains no elements.</summary>
    bool IsEmpty { get; }

    /// <summary>Gets a value indicating whether the current chunk is full.</summary>
    bool IsCompleted { get; }

    /// <summary>
    ///     Starts a fresh buffer for the next chunk.
    /// </summary>
    /// <param name="minimumSize">The minimum number of elements the buffer must hold.</param>
    /// <remarks>
    ///     Must be called before the first chunk and after each <see cref="TryComplete" /> when continuing.
    /// </remarks>
    void Rent(int minimumSize);

    /// <summary>
    ///     Attempts to add an element to the current chunk.
    /// </summary>
    /// <param name="item">The element to add.</param>
    /// <returns>
    ///     <see langword="true" /> if the element was added; otherwise, <see langword="false" /> when the
    ///     chunk is full or no buffer has been rented.
    /// </returns>
    bool TryAdd(T item);

    /// <summary>
    ///     Hands off the current chunk without renting a new buffer.
    /// </summary>
    /// <param name="chunk">The completed chunk, if any.</param>
    /// <returns>
    ///     <see langword="true" /> if a non-empty chunk was handed off; otherwise, <see langword="false" />.
    /// </returns>
    bool TryComplete([MaybeNullWhen(false)] out TChunk chunk);
}

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
        if (_count >= _capacity)
            return false;

        _buffer![_count++] = item;
        return true;
    }

    /// <inheritdoc />
    public bool IsEmpty => _count == 0;

    /// <inheritdoc />
    public bool IsCompleted
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            ArgumentNullException.ThrowIfNull(_buffer);
            return _count >= _capacity;
        }
    }

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
        if (_buffer is not null)
        {
            Pool.Return(_buffer, true);
            _buffer = null;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void Return(T[] buffer)
    {
        Pool.Return(buffer, true);
    }
}

/// <summary>
///     A pool-backed chunk that exposes the accumulated elements as a span.
/// </summary>
/// <typeparam name="T">The type of the accumulated elements.</typeparam>
/// <remarks>
///     The chunk must be disposed exactly once to return the underlying buffer to the pool.
/// </remarks>
[PublicAPI]
public readonly struct Chunk<T> : IDisposable
{
    private readonly T[]? _buffer;
    private readonly int _count;

    /// <summary>Gets the accumulated elements of the chunk.</summary>
    public ReadOnlySpan<T> Span => _buffer is null ? default : _buffer.AsSpan(0, _count);

    internal Chunk(T[] buffer, int count)
    {
        _buffer = buffer;
        _count = count;
    }

    /// <summary>Returns the underlying buffer to the pool.</summary>
    public void Dispose()
    {
        if (_buffer is not null)
            Chunker<T>.Return(_buffer);
    }
}