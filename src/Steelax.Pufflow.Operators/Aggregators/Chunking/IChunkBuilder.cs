using System.Diagnostics.CodeAnalysis;

namespace Steelax.Pufflow.Operators.Aggregators.Chunking;

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