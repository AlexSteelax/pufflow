using System.Runtime.InteropServices;
using Confluent.Kafka;
using Steelax.Pufflow.Operators.Common;
using Steelax.Toolkit.HighPerformance.Primitives;

namespace Steelax.Pufflow.Operators.Kafka;

internal partial class KafkaConsumerProcessor<TKey, TValue>
{
    /// <summary>The pool of progress windows (pre-allocated windows, reused without allocations).</summary>
    private readonly RingCursor<WatermarkStore> _windows;
    
    /// <summary>
    ///     The number of closed windows: incremented when a window is closed,
    ///     decremented when it is flushed (<see cref="FlushReadyWindows" />). While greater than zero,
    ///     the pool contains closed windows awaiting confirmation by the reader's watermark.
    /// </summary>
    private int _closed;

    /// <summary>
    ///     Returns the last (active) window; if no window exists yet, creates the first one and returns it.
    /// </summary>
    /// <returns>A reference to the last window in the pool.</returns>
    private ref WatermarkStore TakeWindow()
    {
        if (_windows.PeekLast(out var lastIndex))
            return ref _windows[lastIndex];

        // No windows yet — reserve the first slot (the window is pre-created by the RingCursor factory).
        if (!_windows.AdvanceLast(out var newIndex))
            throw new InvalidOperationException("Failed to reserve the first window slot.");

        return ref _windows[newIndex];
    }

    /// <summary>
    ///     Attempts to open a new window right after the current one. On success, increments the closed-window
    ///     counter <see cref="_closed" /> (the previous window is considered closed). If the pool is full, does nothing.
    /// </summary>
    /// <remarks>
    ///     Called when the current (tail) window is closed per <see cref="KafkaConsumerOptions.WindowLifetime" />
    ///     and the next one needs to be started. If no free slot is available, new data keeps being written
    ///     into the last window (consolidation).
    /// </remarks>
    private void NextWindow()
    {
        if (!_windows.AdvanceLast(out _))
            return;

        _closed++;
    }
    
    /// <summary>
    ///     A progress window: a dictionary of <see cref="TopicPartitionEpoch" /> → offset plus the maximum
    ///     watermark among the added messages.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         The window is owned by the consumer loop: it writes offsets as messages are emitted.
    ///         The reader never writes into the window directly — it only publishes its watermark
    ///         (via <c>SetReaderWatermark</c>), by which the loop decides when the window can be
    ///         committed (flushed).
    ///     </para>
    ///     <para>
    ///         Window readiness is determined in the processor: the closed-window counter
    ///         (<c>_closed</c>) and the comparison of the window watermark (<see cref="Watermark" />)
    ///         with the reader's watermark (published via <c>SetReaderWatermark</c>). The window is
    ///         flushed when the reader's watermark >= the window watermark. The window itself
    ///         knows nothing about its own state.
    ///     </para>
    /// </remarks>
    private struct WatermarkStore()
    {
        private Watermark _watermark = Watermark.Nothing();
        private readonly Dictionary<TopicPartitionEpoch, Offset> _offsetStore = new();

        /// <summary>The maximum watermark among the messages added to this window.</summary>
        public Watermark Watermark => _watermark;

        /// <summary>Adds an offset to the window and updates the maximum watermark.</summary>
        /// <param name="watermark">The message watermark (monotonic, produced by the processor).</param>
        /// <param name="tpe">The key (topic, partition, epoch).</param>
        /// <param name="offset">The message offset.</param>
        /// <remarks>
        ///     The offset is taken as the max per key within the window: this is correct because offsets
        ///     are strictly monotonic within a single leader epoch.
        /// </remarks>
        public void Add(Watermark watermark, TopicPartitionEpoch tpe, Offset offset)
        {
            if (watermark > _watermark)
                _watermark = watermark;

            ref var last = ref CollectionsMarshal.GetValueRefOrAddDefault(_offsetStore, tpe, out var exists);

            if (!exists || offset > last)
                last = offset;
        }

        /// <summary>Flushes the window through the given advance strategy and resets its state.</summary>
        /// <param name="strategy">The strategy used to commit the offsets to Kafka.</param>
        public void Flush(KafkaAdvanceStrategy strategy)
        {
            var size = _offsetStore.Count;

            if (size == 0)
                return;

            using var enumerator = _offsetStore.GetEnumerator();
            var count = 0;

            var buffer = new TopicPartitionOffset[size];
            while (enumerator.MoveNext())
            {
                var (tpe, offset) = enumerator.Current;
                buffer[count] = new TopicPartitionOffset(tpe.Topic, tpe.Partition, offset, tpe.LeaderEpoch);
                count++;
            }

            strategy.Advance(buffer);

            _watermark = Watermark.Nothing();
            _offsetStore.Clear();
        }
    }
    
    /// <summary>
    ///     A progress key: topic + partition + leader epoch.
    /// </summary>
    /// <remarks>
    ///     The leader epoch participates in the key on par with topic and partition because, after a leader
    ///     re-election, offsets may roll back — progress from different epochs must not be mixed.
    /// </remarks>
    private readonly record struct TopicPartitionEpoch(string Topic, Partition Partition, int? LeaderEpoch)
    {
        /// <summary>Creates a key from a consumed result.</summary>
        [PublicAPI]
        public static TopicPartitionEpoch From(ConsumeResult<TKey, TValue> consumeResult) =>
            new(consumeResult.Topic, consumeResult.Partition, consumeResult.LeaderEpoch);
    }
}
