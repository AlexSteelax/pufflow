using Steelax.Pufflow.Operators.Aggregators.Chunking;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Chunking;

/// <summary>
///     Direct unit tests for <see cref="CarrierChunker{T}" />: the watermark attached to an emitted chunk must be
///     the maximum of all items (data or bare progress) added since the last <see cref="CarrierChunker{T}.TryGet" />,
///     and it must be reset for the next window.
/// </summary>
public class CarrierChunkerTests
{
    [Fact]
    public void TryGet_EmitsMaxWatermark_AndResetsForNextWindow()
    {
        var chunker = new CarrierChunker<int>(4);

        chunker.Add(new Carrier<int>(1, Watermark.From(10)));
        chunker.Add(new Carrier<int>(2, Watermark.From(20)));
        chunker.Add(new Carrier<int>(3, Watermark.From(30)));

        Carrier<Chunk<int>> chunk;
        Assert.True(chunker.TryGet(out chunk));
        Assert.True(chunk.HasValue);

        using (var c = chunk.Value)
            Assert.Equal(new[] { 1, 2, 3 }, c.Span.ToArray());

        Assert.Equal(Watermark.From(30), chunk.Watermark);

        // The window is empty again: nothing may leak into the next one.
        Assert.True(chunker.IsEmpty);

        chunker.Add(new Carrier<int>(4, Watermark.From(5)));

        Carrier<Chunk<int>> next;
        Assert.True(chunker.TryGet(out next));
        Assert.True(next.HasValue);
        Assert.Equal(Watermark.From(5), next.Watermark);

        using (var c = next.Value)
            Assert.Equal(new[] { 4 }, c.Span.ToArray());
    }

    [Fact]
    public void BareItems_DoNotConsumeSlots_ButRaiseTheWatermark()
    {
        var chunker = new CarrierChunker<int>(2);

        chunker.Add(new Carrier<int>(1, Watermark.From(10)));
        chunker.Add(new Carrier<int>(Watermark.From(50)));
        chunker.Add(new Carrier<int>(Watermark.From(60)));

        // Two slots rented, only one data item: the buffer is not full, the bare progress only raised the max.
        Assert.False(chunker.IsFull);

        Carrier<Chunk<int>> chunk;
        Assert.True(chunker.TryGet(out chunk));
        Assert.True(chunk.HasValue);
        Assert.Equal(Watermark.From(60), chunk.Watermark);

        using (var c = chunk.Value)
            Assert.Equal(new[] { 1 }, c.Span.ToArray());
    }

    [Fact]
    public void BareOnly_TryGet_EmitsProgressCarrier_ThenBecomesEmpty()
    {
        var chunker = new CarrierChunker<int>(4);

        chunker.Add(new Carrier<int>(Watermark.From(42)));

        Carrier<Chunk<int>> chunk;
        Assert.True(chunker.TryGet(out chunk));
        Assert.False(chunk.HasValue);
        Assert.Equal(Watermark.From(42), chunk.Watermark);

        // After the bare progress is handed off the chunker is empty: nothing is retained for the next window.
        Assert.True(chunker.IsEmpty);

        // Once drained, TryGet fails instead of re-emitting the same watermark forever.
        Carrier<Chunk<int>> again;
        Assert.False(chunker.TryGet(out again));
    }

    [Fact]
    public void NothingWatermark_IsNotFabricated()
    {
        var chunker = new CarrierChunker<int>(4);

        chunker.Add(new Carrier<int>(1, Watermark.Nothing));

        Carrier<Chunk<int>> chunk;
        Assert.True(chunker.TryGet(out chunk));
        Assert.True(chunk.HasValue);
        Assert.True(chunk.Watermark.IsNothing);
    }

    [Fact]
    public void Dispose_ReturnsBuffer_AndResetsWatermark()
    {
        var chunker = new CarrierChunker<int>(4);

        chunker.Add(new Carrier<int>(1, Watermark.From(10)));
        chunker.Dispose();

        // The disposed watermark must not survive into a fresh window started after Dispose.
        chunker.Add(new Carrier<int>(Watermark.From(5)));

        Carrier<Chunk<int>> chunk;
        Assert.True(chunker.TryGet(out chunk));
        Assert.False(chunk.HasValue);
        Assert.Equal(Watermark.From(5), chunk.Watermark);
    }

    [Fact]
    public void DefaultCarrier_HasNoWatermark_LikeNothing()
    {
        // default(Watermark) is the unbiased 0 sentinel (the true "no watermark" value), so a caller
        // that reads a default slot observes Watermark.Nothing — no fake 0 tick. A carrier built
        // through the library API carries the same sentinel.
        Carrier<Chunk<int>> empty = default;

        Assert.False(empty.HasValue);
        Assert.True(empty.Watermark.IsNothing);
        Assert.Equal(Watermark.Nothing, empty.Watermark);

        var bare = new Carrier<Chunk<int>>(Watermark.Nothing);
        Assert.True(bare.Watermark.IsNothing);
        Assert.Equal(-1L, bare.Watermark);
    }
}