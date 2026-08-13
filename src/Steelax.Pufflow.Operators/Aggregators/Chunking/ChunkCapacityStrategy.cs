namespace Steelax.Pufflow.Operators.Aggregators.Chunking;

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