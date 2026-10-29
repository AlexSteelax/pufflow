using BenchmarkDotNet.Attributes;
using Confluent.Kafka;
using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Operators.Kafka;

namespace Steelax.Pufflow.Benchmarks;

/// <summary>
///     The benchmark seam: the write surface alternative window implementations must provide to participate
///     in <see cref="WatermarkStoreBenchmark{T}" />. It lives in the benchmark project on purpose — the
///     production window stays an internal primitive behind the <see cref="WatermarkStoreAdapter" />.
/// </summary>
public interface IWatermarkStoreWriter
{
    /// <summary>Adds an offset to the window, taking the max per key and updating the maximum watermark.</summary>
    void Add(Watermark watermark, string topic, Partition partition, int? leaderEpoch, Offset offset);

    /// <summary>Resets the window to its empty state, marking all recorded slots as inactive.</summary>
    void Clear();
}

/// <summary>
///     Adapts the production <see cref="WatermarkStore" /> (internal, accessed via InternalsVisibleTo) to
///     <see cref="IWatermarkStoreWriter" />, so the baseline window is benchmarked through the same generic
///     harness as any alternative implementation without exposing it in the package API.
/// </summary>
public struct WatermarkStoreAdapter() : IWatermarkStoreWriter
{
    private WatermarkStore _inner = new();

    public void Add(Watermark watermark, string topic, Partition partition, int? leaderEpoch, Offset offset) =>
        _inner.Add(watermark, new TopicPartitionEpoch(topic, partition, leaderEpoch), offset);

    public void Clear() => _inner.Clear();
}

/// <summary>
///     Benchmarks the hot path of the Kafka progress window: <see cref="IWatermarkStoreWriter.Add" /> driven
///     with <see cref="MessagesPerInvoke" /> messages over a fixed set of unique
///     <see cref="TopicPartitionEpoch" /> groups. The window is pre-filled with all
///     <see cref="Groups" /> keys in setup (outside the measurement), so the measured loop is the steady-state
///     per-message path — mostly updating offsets of already present keys, exactly as the consumer loop sees it.
///     Add alternative implementations by registering them with another <c>[GenericTypeArguments]</c> attribute.
/// </summary>
/// <remarks>
///     <see cref="WatermarkStore.Clear" /> and the struct enumerator are
///     deliberately not measured: they run once per closed window, not per message.
/// </remarks>
[GenericTypeArguments(typeof(WatermarkStoreAdapter))]
[MemoryDiagnoser]
public class WatermarkStoreBenchmark<T>
    where T : IWatermarkStoreWriter, new()
{
    private const int MessagesPerInvoke = 10_000_000;
    private const int ClearsPerInvoke = 1_000_000;

    [Params(1, 10, 100)]
    // ReSharper disable once MemberCanBePrivate.Global
    public int Groups { get; set; } = 1;

    // ReSharper disable once StaticMemberInGenericType
    private static readonly string[] Topics = ["orders", "users", "payments", "analytics"];

    private T _store = default!;
    private TopicPartitionEpoch[] _keys = null!;

    /// <summary>Rebuilds a pre-filled window over all <see cref="Groups" /> unique keys before each iteration.</summary>
    [IterationSetup]
    public void Setup()
    {
        _keys = new TopicPartitionEpoch[Groups];

        for (var i = 0; i < Groups; i++)
        {
            _keys[i] = new TopicPartitionEpoch(
                Topics[i % Topics.Length],
                new Partition(i % 8),
                i / 8);
        }

        _store = new T();

        for (var i = 0; i < Groups; i++)
            _store.Add(Watermark.From(i), _keys[i].Topic, _keys[i].Partition, _keys[i].LeaderEpoch, new Offset(i));
    }

    /// <summary>
    ///     Feeds <see cref="MessagesPerInvoke" /> messages through the window: each message updates the offset
    ///     of one of the unique groups (keys cycle, offsets grow monotonically per group).
    /// </summary>
    [Benchmark(OperationsPerInvoke = MessagesPerInvoke)]
    public void Add()
    {
        var keys = _keys;
        var groups = Groups;
        ref var store = ref _store;

        for (var i = 0; i < MessagesPerInvoke; i++)
        {
            var key = keys[i % groups];
            store.Add(Watermark.From(i), key.Topic, key.Partition, key.LeaderEpoch, new Offset(i));
        }
    }

    /// <summary>
    ///     Resets the window <see cref="ClearsPerInvoke" /> times: the cost of the per-window drain that runs
    ///     once per closed window (marking every slot inactive). The cost is bounded by the window's high-water
    ///     mark <c>_count</c>, not by its current fill, so repeated clears measure the steady drain.
    /// </summary>
    [Benchmark(OperationsPerInvoke = ClearsPerInvoke)]
    public void Clear()
    {
        ref var store = ref _store;

        for (var i = 0; i < ClearsPerInvoke; i++)
            store.Clear();
    }
}