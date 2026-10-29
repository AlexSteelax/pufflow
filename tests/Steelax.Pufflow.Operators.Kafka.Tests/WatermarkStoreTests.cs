using Confluent.Kafka;
using Steelax.Pufflow.Operators.Common;
using Xunit;

namespace Steelax.Pufflow.Operators.Kafka.Tests;

/// <summary>
///     Unit tests for the Kafka progress window <see cref="WatermarkStore" />: aggregation semantics
///     (max offset per key, max watermark), key-space handling (epoch, null epoch, multiple topics),
///     the allocation-free enumerator, and the sorted-index invariants (binary search / insert / growth).
/// </summary>
public class WatermarkStoreTests
{
    private static TopicPartitionEpoch Tpe(string topic, int partition, int? leaderEpoch) =>
        new(topic, new Partition(partition), leaderEpoch);

    private static Offset Off(long value) => new(value);

    /// <summary>Materializes the window's (key, offset) pairs in enumeration order.</summary>
    private static KeyValuePair<TopicPartitionEpoch, Offset>[] Snapshot(WatermarkStore store)
    {
        var result = new List<KeyValuePair<TopicPartitionEpoch, Offset>>();

        foreach (var (tpe, offset) in store)
            result.Add(new(tpe, offset));

        return result.ToArray();
    }

    private static int KeyCompare(TopicPartitionEpoch x, TopicPartitionEpoch y)
    {
        var compare = x.Partition.Value.CompareTo(y.Partition.Value);

        if (compare != 0)
            return compare;

        compare = (x.LeaderEpoch ?? -1).CompareTo(y.LeaderEpoch ?? -1);

        return compare != 0 ? compare : string.CompareOrdinal(x.Topic, y.Topic);
    }

    private static readonly Comparison<KeyValuePair<TopicPartitionEpoch, Offset>> ByKey =
        (x, y) => KeyCompare(x.Key, y.Key);

    [Fact]
    public void Add_NewKeys_AppendsEntriesAndTracksCount()
    {
        var store = new WatermarkStore();

        Assert.True(store.IsEmpty);
        Assert.Equal(0, store.Count);

        store.Add(Watermark.From(1), Tpe("orders", 0, 1), Off(100));
        store.Add(Watermark.From(2), Tpe("orders", 1, 1), Off(200));

        Assert.False(store.IsEmpty);
        Assert.Equal(2, store.Count);
        Assert.Equal(Watermark.From(2), store.Watermark);
    }

    [Fact]
    public void Add_SameKey_TakesMaximumOffset()
    {
        var store = new WatermarkStore();
        var key = Tpe("orders", 0, 1);

        store.Add(Watermark.From(1), key, Off(10));
        store.Add(Watermark.From(2), key, Off(5)); // lower — ignored
        store.Add(Watermark.From(3), key, Off(7)); // still below 10
        store.Add(Watermark.From(4), key, Off(42)); // new max

        Assert.Equal(1, store.Count);
        Assert.Equal(new Offset(42), Lookup(store, key));
    }

    /// <summary>Gets the offset of the single pair of a one-entry window.</summary>
    private static Offset Single(WatermarkStore store)
    {
        var pairs = Snapshot(store);
        Assert.Single(pairs);
        return pairs[0].Value;
    }

    [Fact]
    public void Add_SameKeyWithHigherAndLower_KeepsMax()
    {
        var store = new WatermarkStore();
        var key = Tpe("orders", 0, 1);

        store.Add(Watermark.From(1), key, Off(5));
        Assert.Equal(Off(5), Single(store));

        store.Add(Watermark.From(2), key, Off(9));
        Assert.Equal(Off(9), Single(store));

        store.Add(Watermark.From(3), key, Off(2));
        Assert.Equal(Off(9), Single(store));
    }

    [Fact]
    public void Add_SameTopicAndPartition_DifferentEpochs_AreDistinctKeys()
    {
        var store = new WatermarkStore();

        store.Add(Watermark.From(1), Tpe("orders", 0, 1), Off(100));
        store.Add(Watermark.From(2), Tpe("orders", 0, 2), Off(200));

        Assert.Equal(2, store.Count);
    }

    [Fact]
    public void Add_NullEpoch_IsDistinctFromNumberedEpoch()
    {
        var store = new WatermarkStore();

        store.Add(Watermark.From(1), Tpe("orders", 0, null), Off(100));
        store.Add(Watermark.From(1), Tpe("orders", 0, 0), Off(200));

        Assert.Equal(2, store.Count);
    }

    [Fact]
    public void Watermark_IsMaximumOfSeenWatermarks()
    {
        var store = new WatermarkStore();

        store.Add(Watermark.From(5), Tpe("orders", 0, 1), Off(1));
        store.Add(Watermark.From(1), Tpe("orders", 1, 1), Off(2));
        store.Add(Watermark.From(3), Tpe("orders", 2, 1), Off(3));

        Assert.Equal(Watermark.From(5), store.Watermark);
    }

    [Fact]
    public void Clear_ResetsStateAndAllowsReuse()
    {
        var store = new WatermarkStore();
        store.Add(Watermark.From(5), Tpe("orders", 0, 1), Off(10));

        store.Clear();

        Assert.True(store.IsEmpty);
        Assert.Equal(0, store.Count);
        Assert.Equal(Watermark.Nothing, store.Watermark);

        // The window must be reusable after a clear (ring of windows).
        store.Add(Watermark.From(7), Tpe("orders", 0, 1), Off(20));
        Assert.Equal(Off(20), Single(store));
        Assert.Equal(Watermark.From(7), store.Watermark);
    }

    [Fact]
    public void GetEnumerator_YieldsPairsInKeyOrder_PartitionEpochTopic()
    {
        var store = new WatermarkStore();

        store.Add(Watermark.From(1), Tpe("users", 2, 1), Off(1));
        store.Add(Watermark.From(1), Tpe("orders", 0, 1), Off(2));
        store.Add(Watermark.From(1), Tpe("orders", 1, 2), Off(3));
        store.Add(Watermark.From(1), Tpe("orders", 1, 1), Off(4));
        store.Add(Watermark.From(1), Tpe("orders", 0, null), Off(5));

        var pairs = Snapshot(store);

        Assert.Equal(5, pairs.Length);
        AssertOrdered(pairs);
        Assert.Equal(Tpe("orders", 0, null), pairs[0].Key);
    }

    [Fact]
    public void GetEnumerator_RoundTripsNullLeaderEpoch()
    {
        var store = new WatermarkStore();
        store.Add(Watermark.From(1), Tpe("orders", 3, null), Off(7));

        var (tpe, offset) = Assert.Single(Snapshot(store));
        Assert.Null(tpe.LeaderEpoch);
        Assert.Equal(Off(7), offset);
    }

    [Fact]
    public void Insert_BetweenExistingEntries_StaysSortedAndSearchable()
    {
        var store = new WatermarkStore();

        store.Add(Watermark.From(1), Tpe("orders", 0, 1), Off(1));
        store.Add(Watermark.From(1), Tpe("orders", 4, 1), Off(2));

        store.Add(Watermark.From(1), Tpe("orders", 2, 1), Off(3)); // middle insert

        Assert.Equal(3, store.Count);
        AssertOrdered(Snapshot(store));

        // The inserted key is findable and updatable afterwards.
        store.Add(Watermark.From(2), Tpe("orders", 2, 1), Off(30));
        Assert.Equal(3, store.Count);
        Assert.Equal(new Offset(30), Lookup(store, Tpe("orders", 2, 1)));
    }

    [Fact]
    public void Insert_BeyondInitialCapacity_GrowsAndStaysSorted()
    {
        const int keys = 40;
        var store = new WatermarkStore();

        // Insert in a shuffled order to exercise both the tail-append and middle-insert paths.
        var order = Enumerable.Range(0, keys).OrderBy(_ => Random.Shared.Next()).ToArray();

        foreach (var i in order)
            store.Add(Watermark.From(i), Tpe("orders", i, 1), Off(i * 10));

        Assert.Equal(keys, store.Count);

        var pairs = Snapshot(store);
        AssertOrdered(pairs);

        // Max-offset semantics hold after growth.
        store.Add(Watermark.From(keys), Tpe("orders", 3, 1), Off(999));
        store.Add(Watermark.From(keys + 1), Tpe("orders", 3, 1), Off(3));
        Assert.Equal(new Offset(999), Lookup(store, Tpe("orders", 3, 1)));
    }

    [Fact]
    public void Add_DifferentTopics_SamePartitionAndEpoch_TieBreaksByTopic()
    {
        var store = new WatermarkStore();

        store.Add(Watermark.From(1), Tpe("orders", 0, 1), Off(1));
        store.Add(Watermark.From(1), Tpe("users", 0, 1), Off(2));

        Assert.Equal(2, store.Count);

        var pairs = Snapshot(store);
        AssertOrdered(pairs);
        Assert.Equal("orders", pairs[0].Key.Topic);
        Assert.Equal("users", pairs[1].Key.Topic);
    }

    [Fact]
    public void Clear_ThenReAddSubset_CountsAndEnumeratesOnlyActive()
    {
        var store = new WatermarkStore();

        store.Add(Watermark.From(1), Tpe("orders", 0, 1), Off(10));
        store.Add(Watermark.From(2), Tpe("orders", 1, 1), Off(20));
        store.Add(Watermark.From(3), Tpe("orders", 2, 1), Off(30));

        store.Clear();

        // Only two of the three keys reappear in the next window.
        store.Add(Watermark.From(4), Tpe("orders", 0, 1), Off(40));
        store.Add(Watermark.From(5), Tpe("orders", 2, 1), Off(50));

        Assert.Equal(2, store.Count);
        Assert.Equal(Watermark.From(5), store.Watermark);

        var pairs = Snapshot(store);
        Assert.Equal(2, pairs.Length);
        Assert.Equal(Tpe("orders", 0, 1), pairs[0].Key);
        Assert.Equal(Off(40), pairs[0].Value);
        Assert.Equal(Tpe("orders", 2, 1), pairs[1].Key);
        Assert.Equal(Off(50), pairs[1].Value);
    }

    [Fact]
    public void Clear_ThenReAddExistingKeys_ReactivateSlotsWithFreshMaxPerWindow()
    {
        var store = new WatermarkStore();
        var key = Tpe("orders", 0, 1);

        store.Add(Watermark.From(1), key, Off(100));
        store.Clear();

        // The reactivated slot starts a fresh window: a lower offset wins again (max semantics per window).
        store.Add(Watermark.From(2), key, Off(5));
        Assert.Equal(Off(5), Single(store));

        store.Add(Watermark.From(3), key, Off(7));
        store.Add(Watermark.From(4), key, Off(3));
        Assert.Equal(Off(7), Single(store));
        Assert.Equal(1, store.Count);
    }

    [Fact]
    public void Clear_ThenInsertNewKeyBetweenStaleSlots_StaysOrderedAndSearchable()
    {
        var store = new WatermarkStore();

        store.Add(Watermark.From(1), Tpe("orders", 0, 1), Off(1));
        store.Add(Watermark.From(1), Tpe("orders", 4, 1), Off(2));

        store.Clear();

        // A new (never seen) key is inserted between the stale slots; the stale one stays inactive.
        store.Add(Watermark.From(2), Tpe("orders", 2, 1), Off(3));
        store.Add(Watermark.From(2), Tpe("orders", 0, 1), Off(10));

        Assert.Equal(2, store.Count);

        var pairs = Snapshot(store);
        AssertOrdered(pairs);
        Assert.Equal(Tpe("orders", 0, 1), pairs[0].Key);
        Assert.Equal(Tpe("orders", 2, 1), pairs[1].Key);

        // Reactivated and inserted keys are findable and updatable afterwards.
        store.Add(Watermark.From(3), Tpe("orders", 2, 1), Off(42));
        store.Add(Watermark.From(3), Tpe("orders", 4, 1), Off(50));

        Assert.Equal(3, store.Count);
        Assert.Equal(new Offset(50), Lookup(store, Tpe("orders", 4, 1)));
        Assert.Equal(new Offset(42), Lookup(store, Tpe("orders", 2, 1)));
    }

    [Fact]
    public void Clear_MultipleCycles_SaturationReusesSlots()
    {
        var store = new WatermarkStore();
        var keys = Enumerable.Range(0, 10).Select(p => Tpe("orders", p, 1)).ToArray();

        // First cycle: pure inserts.
        foreach (var key in keys)
            store.Add(Watermark.From(1), key, Off(1));

        Assert.Equal(keys.Length, store.Count);

        for (var cycle = 0; cycle < 3; cycle++)
        {
            store.Clear();
            Assert.True(store.IsEmpty);
            Assert.Equal(0, store.Count);
            Assert.Equal(Watermark.Nothing, store.Watermark);

            // Second and later cycles: all slots reactivate without growth.
            foreach (var key in keys)
                store.Add(Watermark.From(cycle + 2), key, Off((cycle + 1) * 100));

            Assert.Equal(keys.Length, store.Count);
            AssertOrdered(Snapshot(store));

            foreach (var key in keys)
                Assert.Equal(new Offset((cycle + 1) * 100), Lookup(store, key));
        }
    }

    private static Offset Lookup(WatermarkStore store, TopicPartitionEpoch key)
    {
        foreach (var (tpe, offset) in store)
        {
            if (tpe == key)
                return offset;
        }

        Assert.Fail($"key {key} is not present in the window");
        return default;
    }

    private static void AssertOrdered(KeyValuePair<TopicPartitionEpoch, Offset>[] pairs)
    {
        for (var i = 1; i < pairs.Length; i++)
            Assert.True(
                KeyCompare(pairs[i - 1].Key, pairs[i].Key) < 0,
                $"pairs[{i - 1}] ({pairs[i - 1].Key}) must sort before pairs[{i}] ({pairs[i].Key})");
    }
}