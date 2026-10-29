using System.Runtime.InteropServices;
using Confluent.Kafka;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Kafka;

/// <summary>
///     A progress window: a dictionary of <see cref="TopicPartitionEpoch" /> → offset plus the maximum
///     watermark among the added messages.
/// </summary>
/// <remarks>
    ///     <para>
    ///         The window is a pure data container owned by the consumer loop: it accumulates offsets as messages
    ///         are emitted and exposes its content through an allocation-free struct enumerator. Committing the
    ///         collected offsets to Kafka is deliberately <em>not</em> its concern — that is done by
    ///         <see cref="KafkaAdvanceStrategy.Flush" />, after which the owner resets the window for reuse via
    ///         <see cref="Clear" />.
    ///     </para>
///     <para>
///         Window readiness is determined by the owning processor: the closed-window counter and the
///         comparison of the window watermark (<see cref="Watermark" />) with the reader's watermark
///         (published via <see cref="WatermarkTracker" />). The window itself knows nothing about its own
///         state.
///     </para>
/// </remarks>
[PublicAPI]
internal struct WatermarkStore()
{
    private Watermark _watermark = Watermark.Nothing;
    private readonly Dictionary<TopicPartitionEpoch, Offset> _offsetStore = new();

    /// <summary>The maximum watermark among the messages added to this window.</summary>
    public Watermark Watermark => _watermark;

    /// <summary>The number of distinct keys recorded in this window.</summary>
    public int Count => _offsetStore.Count;

    /// <summary>Gets whether the window holds no recorded offsets.</summary>
    public bool IsEmpty => _offsetStore.Count == 0;

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

    /// <summary>Resets the window to its empty state, releasing all recorded offsets.</summary>
    public void Clear()
    {
        _watermark = Watermark.Nothing;
        _offsetStore.Clear();
    }

    /// <summary>Returns an allocation-free enumerator over the recorded <c>(key, offset)</c> pairs.</summary>
    public Enumerator GetEnumerator() => new(_offsetStore.GetEnumerator());

    /// <summary>An allocation-free, forward-only enumerator over the window's recorded offsets.</summary>
    public struct Enumerator
    {
        private Dictionary<TopicPartitionEpoch, Offset>.Enumerator _inner;

        internal Enumerator(Dictionary<TopicPartitionEpoch, Offset>.Enumerator inner) => _inner = inner;

        /// <summary>Advances the enumerator to the next recorded pair.</summary>
        public bool MoveNext() => _inner.MoveNext();

        /// <summary>Gets the current <c>(key, offset)</c> pair.</summary>
        public KeyValuePair<TopicPartitionEpoch, Offset> Current => _inner.Current;
    }
}

/// <summary>
///     A progress key: topic + partition + leader epoch.
/// </summary>
/// <remarks>
///     The leader epoch participates in the key on par with topic and partition because, after a leader
///     re-election, offsets may roll back — progress from different epochs must not be mixed.
/// </remarks>
[PublicAPI]
internal readonly record struct TopicPartitionEpoch(string Topic, Partition Partition, int? LeaderEpoch);