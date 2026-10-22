using Steelax.Pufflow.Operators.Aggregators.Chunking;
using Steelax.Pufflow.Sdk.Test;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Chunking;

/// <summary>
///     Black-box tests for the <c>Chunking(minimumSize, linger)</c> operator (backed by
///     <see cref="ChunkProcessor{T,TChunk}" />): an async consumator source feeds elements through the
///     chunker and the resulting <see cref="Chunk{T}" /> stream is observed downstream.
/// </summary>
public static partial class ChunkProcessorTests
{
    private const int TimeoutMs = 2_000;
}
