using Confluent.Kafka;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Kafka;

/// <summary>
///     A progress window: an ordinal-sorted index of <see cref="TopicPartitionEpoch" /> → offset plus the
///     maximum watermark among the added messages.
/// </summary>
/// <remarks>
///     <para>
///         The window keeps the sorted keys and the offsets in two parallel arrays (<b>structure of
///         arrays</b>): a binary search for an incoming message's slot touches only the key array, while the
///         offset of the hottest writes and the per-window drain live in one contiguous <see cref="long" />
///         buffer. Keys are ordered by <c>(partition, leader epoch, topic)</c>: the typical scenario is a
///         single topic, so the int-typed partition (then epoch) tells the entries apart immediately and the
///         topic string is only compared on the rare tie path.
///     </para>
///     <para>
///         To reuse the arrays across windows without per-window rebuild, a clear does not drop the keys: it
///         rewinds <c>_active</c> to zero and fills the offsets' high-water region with
///         <see cref="Offset.Unset" /> (a vectorizable O(n) fill, once per closed window), while <c>_count</c>
///         keeps the number of distinct keys <em>ever</em> seen. Once the window saturates, an
///         <see cref="Add" /> for a key seen in an earlier window just reactivates its slot — a binary search
///         plus a single offset write, with no memmove and no rehash — and only genuinely new keys (e.g. a
///         leader-epoch change) pay the sorted insert.
///     </para>
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
    private const int InitialCapacity = 4;

    /// <summary>The liveness sentinel: message offsets are non-negative, so a negative slot marks an inactive key.</summary>
    private static readonly long UnsetOffset = Offset.Unset.Value;

    private Watermark _watermark = Watermark.Nothing;
    private Key[] _keys = new Key[InitialCapacity];
    private long[] _offsets = new long[InitialCapacity];

    /// <summary>The number of distinct keys ever seen (the arrays' high-water mark, including inactive slots).</summary>
    private int _count;

    /// <summary>The number of keys holding a real offset in the current window.</summary>
    private int _active;

    /// <summary>The maximum watermark among the messages added to this window.</summary>
    public Watermark Watermark => _watermark;

    /// <summary>The number of distinct keys recorded in this window.</summary>
    public int Count => _active;

    /// <summary>Gets whether the window holds no recorded offsets.</summary>
    public bool IsEmpty => _active == 0;

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

        var epoch = tpe.LeaderEpoch ?? -1;
        var index = Find(tpe.Topic, tpe.Partition.Value, epoch);

        if (index >= 0)
        {
            var lastValue = _offsets[index];

            // The slot is reused from an earlier window: a negative value identifies the Unset sentinel and
            // reactivating the slot counts the key as live again. Activating folds into the write below; an
            // active slot with a not-newer offset is a no-op.
            if (lastValue < 0)
                _active++;
            else if (offset.Value <= lastValue)
                return;

            _offsets[index] = offset.Value;
            return;
        }

        Insert(~index, tpe.Topic, tpe.Partition.Value, epoch, offset);
    }

    /// <summary>Resets the window to its empty state, marking all recorded slots as inactive.</summary>
    /// <remarks>
    ///     The keys and the backing arrays are kept: the offsets' high-water region is filled with
    ///     <see cref="Offset.Unset" /> (<c>_count</c> unchanged) and only the active-count is rewound, so a
    ///     saturated window stops growing after its first cycle and the next window reactivates the same slots
    ///     instead of memmoving or rehashing.
    /// </remarks>
    public void Clear()
    {
        _watermark = Watermark.Nothing;
        Array.Fill(_offsets, UnsetOffset, 0, _count);
        _active = 0;
    }

    /// <summary>Returns an allocation-free enumerator over the live <c>(key, offset)</c> pairs, in key order.</summary>
    public Enumerator GetEnumerator() => new(_keys, _offsets, _count);

    /// <summary>Finds the slot of the given key; a negative result is the bitwise complement of the insertion point.</summary>
    private int Find(string topic, int partition, int epoch)
    {
        var lo = 0;
        var hi = _count - 1;

        while (lo <= hi)
        {
            var mid = lo + ((hi - lo) >> 1);
            var compare = Compare(ref _keys[mid], topic, partition, epoch);

            if (compare < 0)
                lo = mid + 1;
            else if (compare > 0)
                hi = mid - 1;
            else
                return mid;
        }

        return ~lo;
    }

    private static int Compare(ref Key key, string topic, int partition, int epoch)
    {
        var compare = key.Partition.CompareTo(partition);

        if (compare != 0)
            return compare;

        compare = key.Epoch.CompareTo(epoch);

        // The topic string is reached only when both int-typed components tie: the same (partition, epoch)
        // appearing under different topic names (multi-topic consumer). Ordinal compare keeps a total order.
        return compare != 0 ? compare : string.CompareOrdinal(key.Topic, topic);
    }

    private void Insert(int index, string topic, int partition, int epoch, Offset offset)
    {
        if (_count == _keys.Length)
        {
            Array.Resize(ref _keys, _keys.Length * 2);
            Array.Resize(ref _offsets, _offsets.Length * 2);
        }

        if (index < _count)
        {
            Array.Copy(_keys, index, _keys, index + 1, _count - index);
            Array.Copy(_offsets, index, _offsets, index + 1, _count - index);
        }

        _keys[index] = new Key(partition, epoch, topic);
        _offsets[index] = offset.Value;
        _count++;
        _active++;
    }

    /// <summary>
    ///     An allocation-free, forward-only enumerator over the window's offsets holding a value, in key order.
    ///     Inactive slots (offsets reset to <see cref="Offset.Unset" /> by <see cref="WatermarkStore.Clear" />)
    ///     are skipped.
    /// </summary>
    public struct Enumerator
    {
        private readonly Key[] _keys;
        private readonly long[] _offsets;

        /// <summary>The arrays' high-water mark; the enumerator scans up to here, skipping inactive slots.</summary>
        private readonly int _count;

        private int _index;

        internal Enumerator(Key[] keys, long[] offsets, int count)
        {
            _keys = keys;
            _offsets = offsets;
            _count = count;
            _index = -1;
        }

        /// <summary>Advances the enumerator to the next live pair.</summary>
        public bool MoveNext()
        {
            // ReSharper disable once EmptyEmbeddedStatement
            while (++_index < _count && _offsets[_index] < 0) { }

            return _index < _count;
        }

        /// <summary>Gets the current <c>(key, offset)</c> pair.</summary>
        public KeyValuePair<TopicPartitionEpoch, Offset> Current
        {
            get
            {
                ref var key = ref _keys[_index];
                return new KeyValuePair<TopicPartitionEpoch, Offset>(
                    new TopicPartitionEpoch(key.Topic, key.Partition, key.Epoch == -1 ? null : key.Epoch),
                    new Offset(_offsets[_index]));
            }
        }
    }

    internal readonly struct Key(int partition, int epoch, string topic)
    {
        public readonly int Partition = partition;
        public readonly int Epoch = epoch;
        public readonly string Topic = topic;
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