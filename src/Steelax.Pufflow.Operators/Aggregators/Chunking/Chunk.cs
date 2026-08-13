using System.Collections;

namespace Steelax.Pufflow.Operators.Aggregators.Chunking;

/// <summary>
///     A pool-backed chunk that exposes the accumulated elements as a span or via enumeration.
/// </summary>
/// <typeparam name="T">The type of the accumulated elements.</typeparam>
/// <remarks>
///     <para>
///         The chunk must be disposed exactly once to return the underlying buffer to the pool. After
///         disposal the chunk (and any enumerator obtained from it) no longer references the buffer.
///     </para>
///     <para>
///         Enumeration allocates no per-iteration objects: a single <see cref="ChunkEnumerator" /> is
///         reused per thread (<see cref="System.ThreadStaticAttribute" />) and re-initialized on each
///         <see cref="GetEnumerator" /> call. Enumerate only while the chunk is alive (before
///         <see cref="Dispose" />); concurrent enumeration of two chunks on the same thread is not
///         supported.
///     </para>
/// </remarks>
[PublicAPI]
public readonly struct Chunk<T> : IDisposable, IEnumerable<T>
{
    [ThreadStatic]
    private static ChunkEnumerator? _enumerator;
    
    private readonly T[]? _buffer;
    private readonly int _count;

    /// <summary>Gets the accumulated elements of the chunk as a read-only span.</summary>
    public ReadOnlySpan<T> Span => _buffer is null ? default : _buffer.AsSpan(0, _count);

    internal Chunk(T[] buffer, int count)
    {
        _buffer = buffer;
        _count = count;
    }

    /// <summary>
    ///     Returns the underlying buffer to the pool and releases the thread-local enumerator (if one was
    ///     obtained from this chunk). Safe to call once; the chunk becomes empty afterwards.
    /// </summary>
    public void Dispose()
    {
        if (_buffer is not null)
            Chunker<T>.Return(_buffer);
        
        _enumerator?.Dispose();
    }

    /// <summary>
    ///     Returns a reusable enumerator over the chunk's elements. The enumerator is cached per thread and
    ///     re-initialized on every call, so enumeration does not allocate. The chunk must be enumerated
    ///     before <see cref="Dispose" />.
    /// </summary>
    /// <returns>An enumerator over the accumulated elements.</returns>
    public ChunkEnumerator GetEnumerator()
    {
        _enumerator ??= new ChunkEnumerator();
        _enumerator.Init(_buffer ?? [], _count);
        return _enumerator;
    }

    IEnumerator<T> IEnumerable<T>.GetEnumerator() => GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    
    /// <summary>
    ///     A reusable enumerator over a <see cref="Chunk{T}" />'s elements. Obtained from
    ///     <see cref="Chunk{T}.GetEnumerator" /> and re-initialized on each call, so enumeration does not
    ///     allocate. Not thread-safe; only enumerate a chunk while it is alive (before
    ///     <see cref="Chunk{T}.Dispose" />).
    /// </summary>
    [PublicAPI]
    public sealed class ChunkEnumerator : IEnumerator<T>
    {
        private T[] _buffer = null!;
        private int _count = -1;

        private int _cursor = -1;
        
        private T _current = default!;

        internal void Init(T[] buffer, int count)
        {
            _buffer = buffer;
            _count = count;
            _cursor = 0;
            _current = default!;
        }

        /// <summary>Advances the enumerator to the next element.</summary>
        /// <returns>
        ///     <see langword="true" /> if the enumerator advanced to a valid element; <see langword="false" />
        ///     when the end of the chunk is reached.
        /// </returns>
        public bool MoveNext()
        {
            if (_cursor == _count)
            {
                _current = default!;
                return false;
            }
            
            _current = _buffer[_cursor++];
            return true;
        }

        /// <summary>Resets the enumerator to the start of the chunk.</summary>
        public void Reset()
        {
            _cursor = 0;
            _current = default!;
        }

        /// <summary>Gets the element at the current position of the enumerator.</summary>
        public object? Current => _current;

        T IEnumerator<T>.Current => _current;

        object? IEnumerator.Current => _current;

        /// <summary>Releases the enumerator's reference to the chunk buffer.</summary>
        public void Dispose()
        {
            _buffer = null!;
            _count = -1;
            _cursor = -1;
            _current = default!;
        }
    }
}