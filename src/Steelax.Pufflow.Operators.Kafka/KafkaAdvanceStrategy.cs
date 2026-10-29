using Celerity.Collections;
using Confluent.Kafka;

namespace Steelax.Pufflow.Operators.Kafka;

/// <summary>
/// Advances committed offsets back to Kafka after records are processed.
/// </summary>
internal abstract class KafkaAdvanceStrategy
{
    private readonly Deque<TopicPartitionOffset> _buffer = new();
    
    /// <summary>
    /// Advances the committed position for the given offsets.
    /// </summary>
    /// <param name="offsets">Per-partition offsets ready to be committed.</param>
    protected abstract void Advance<TCollection>(TCollection offsets)
        where TCollection : IEnumerable<TopicPartitionOffset>;

    /// <summary>
    /// Drains a closed progress window: materializes its recorded offsets into
    /// <see cref="TopicPartitionOffset" /> entries and advances them via <see cref="Advance" />.
    /// </summary>
    /// <remarks>
    ///     The window state is left untouched: the caller resets it via
    ///     <see cref="WatermarkStore.Clear" /> after a successful advance, so a failure leaves the
    ///     recorded offsets intact for a retry.
    /// </remarks>
    /// <param name="window">The closed window to drain.</param>
    public void Flush(ref WatermarkStore window)
    {
        if (window.IsEmpty)
            return;

        foreach (var (tpe, offset) in window)
            _buffer.PushBack(new TopicPartitionOffset(tpe.Topic, tpe.Partition, offset, tpe.LeaderEpoch));

        // The buffer must be drained even when the advance fails: the window is not reset on error
        // (the caller retries it), so a stale buffer would double the recorded offsets on the next flush.
        try
        {
            Advance(_buffer);
        }
        finally
        {
            _buffer.Clear();
        }
    }
}