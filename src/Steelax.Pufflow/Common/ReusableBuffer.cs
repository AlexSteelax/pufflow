using System.Collections;
using System.Runtime.CompilerServices;

// ReSharper disable ConvertToAutoPropertyWithPrivateSetter

namespace Steelax.Pufflow.Common;

/// <summary>
///     Represents a reusable fixed-capacity buffer that implements both <see cref="IEnumerable{T}" /> and
///     <see cref="IEnumerator{T}" />.
///     This buffer is designed for zero-allocation batching scenarios where data is accumulated
///     and then consumed via enumeration, after which the buffer can be reset and reused.
/// </summary>
/// <typeparam name="T">The type of elements stored in the buffer. Supports <see langword="ref struct" /> types.</typeparam>
/// <remarks>
///     <para>
///         The same instance serves as both the enumerable and the enumerator, avoiding a separate allocation
///         when enumerated via <see langword="foreach" />.
///     </para>
///     <para>
///         This type is intended for bridging between push-based data sources (e.g., async streams)
///         and pull-based sync consumers via batch processing.
///     </para>
///     <para>This type is not thread-safe.</para>
/// </remarks>
internal sealed class ReusableBuffer<T>(int capacity) : IEnumerable<T>, IEnumerator<T>
{
    private static readonly bool IsReferenceOrContainsReferences = RuntimeHelpers.IsReferenceOrContainsReferences<T>();

    private readonly T[] _buffer = new T[capacity];

    private int _count;
    private int _index = -1;

    /// <summary>
    ///     Gets the number of elements currently stored in the buffer.
    /// </summary>
    /// <value>
    ///     The number of elements added via <see cref="TryAdd" /> since the last <see cref="Reset" /> or
    ///     <see cref="Dispose" />.
    /// </value>
    [PublicAPI]
    public int Count => _count;

    IEnumerator<T> IEnumerable<T>.GetEnumerator()
    {
        return GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    /// <summary>
    ///     Resets the buffer to its initial empty state, allowing it to be reused.
    /// </summary>
    /// <remarks>
    ///     If <typeparamref name="T" /> is a reference type or contains references, the underlying array
    ///     is cleared only when the buffer is already empty to avoid holding unnecessary references.
    ///     When the buffer contains items, only the internal counters are reset; the array elements
    ///     will be overwritten on subsequent <see cref="TryAdd" /> calls.
    /// </remarks>
    [PublicAPI]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Reset()
    {
        if (_count != 0 && IsReferenceOrContainsReferences)
            Array.Clear(_buffer);

        _index = -1;
        _count = 0;
    }

    /// <summary>
    ///     Gets the element at the current position of the enumerator.
    /// </summary>
    /// <value>The element at the current enumeration position.</value>
    [PublicAPI]
    public T Current => _buffer[_index];

    /// <summary>
    ///     Advances the enumerator to the next element of the buffer.
    /// </summary>
    /// <returns>
    ///     <see langword="true" /> if the enumerator successfully advanced to the next element;
    ///     <see langword="false" /> if the end of the buffer has been passed.
    /// </returns>
    [PublicAPI]
    public bool MoveNext()
    {
        return ++_index < _count;
    }

    /// <summary>
    ///     Releases the resources held by the buffer.
    /// </summary>
    /// <remarks>
    ///     If <typeparamref name="T" /> is a reference type or contains references, the underlying
    ///     array is cleared to release references to the stored elements. The buffer should not
    ///     be used after disposal.
    /// </remarks>
    [PublicAPI]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose()
    {
        if (IsReferenceOrContainsReferences)
            Array.Clear(_buffer);
    }

    object? IEnumerator.Current => Current;

    /// <summary>
    ///     Attempts to add an item to the buffer.
    /// </summary>
    /// <param name="item">The item to add.</param>
    /// <returns>
    ///     <see langword="true" /> if the item was successfully added; otherwise, <see langword="false" /> if the buffer
    ///     is full.
    /// </returns>
    [PublicAPI]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryAdd(T item)
    {
        if (_count >= _buffer.Length)
            return false;

        _buffer[_count++] = item;
        return true;
    }

    /// <summary>
    ///     Returns a read-only span over the currently buffered elements.
    /// </summary>
    /// <returns>A <see cref="ReadOnlySpan{T}" /> containing all elements in the buffer.</returns>
    /// <remarks>
    ///     This method provides direct memory access to the buffered data without requiring iteration.
    ///     The span is valid only until the next <see cref="Reset" /> or <see cref="Dispose" /> call.
    /// </remarks>
    [PublicAPI]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<T> AsSpan()
    {
        return _buffer.AsSpan(0, _count);
    }

    /// <summary>
    ///     Returns the current buffer instance as an enumerator.
    /// </summary>
    /// <returns>
    ///     The same <see cref="ReusableBuffer{T}" /> instance, enabling reuse as both <see cref="IEnumerable{T}" /> and
    ///     <see cref="IEnumerator{T}" />.
    /// </returns>
    /// <remarks>
    ///     This method returns <see langword="this" /> rather than creating a new enumerator,
    ///     which eliminates allocation overhead. However, this means nested or concurrent iterations
    ///     on the same instance are not supported.
    /// </remarks>
    [PublicAPI]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReusableBuffer<T> GetEnumerator()
    {
        return this;
    }
}