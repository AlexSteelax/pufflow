using System.Runtime.CompilerServices;
using Steelax.Toolkit.HighPerformance;
using Steelax.Toolkit.HighPerformance.Concurrency.Primitives;

namespace Steelax.Pufflow.Operators.Common;

/// <summary>
///     A shared, thread-safe monotonic progress point reported by the reader of a stream and read back by its
///     sources. This is the wiring between pipeline stages and the entities that own commit state: a downstream
///     reader publishes the watermark it has processed (<see cref="SetProgress" />), while an upstream source
///     (such as a Kafka consumer processor) reads the confirmed progress (<see cref="GetProgress" />) to decide
///     when closed windows can be flushed.
/// </summary>
/// <remarks>
///     A single tracker instance can be shared between several sources — for example all inputs of a merge —
///     so that the merged readers' watermark is visible to every contributing source at once.
///     <para />
///     The tracker is purely a <c>max</c> condenser: <see cref="SetProgress" /> only ever advances the stored
///     value, and <see cref="GetProgress" /> never returns a value smaller than a previously observed one. Values
///     below or equal to <see cref="Watermark.Nothing" /> (special watermarks) carry no progress and are ignored.
///     <para />
///     Implementations differ in how far they go to avoid contention between concurrent writers: a single-slot
///     instance is enough when one writer calls <see cref="SetProgress" />, while the sharded variants
///     distribute writer updates across cache-line-aligned slots for many-writer scenarios. Readers are always
///     lock-free.
/// </remarks>
[PublicAPI]
public abstract class WatermarkTracker
{
    /// <summary>
    ///     Publishes a new progress point. Safe to call from any thread; the tracker only ever moves its value
    ///     forward (the maximum of all reported watermarks).
    /// </summary>
    /// <param name="watermark">The watermark up to which the reader has processed data.</param>
    /// <remarks>
    ///     Special watermarks (at or below <see cref="Watermark.Nothing" />) are ignored.
    /// </remarks>
    [PublicAPI]
    public abstract void SetProgress(Watermark watermark);

    /// <summary>
    ///     Returns the newest progress point published so far, or <see cref="Watermark.Nothing" /> when no real
    ///     watermark has been reported yet. Safe to call from any thread.
    /// </summary>
    /// <returns>The maximum watermark ever published via <see cref="SetProgress" />.</returns>
    [PublicAPI]
    public abstract Watermark GetProgress();

    /// <summary>
    ///     Creates a <see cref="WatermarkTracker" /> tuned for the expected writer concurrency.
    /// </summary>
    /// <param name="concurrency">
    ///     The expected write contention: <see cref="WatermarkTrackerConcurrency.None" /> for a single writer,
    ///     <see cref="WatermarkTrackerConcurrency.Low" /> for light writer concurrency (default), and
    ///     <see cref="WatermarkTrackerConcurrency.Extreme" /> for many simultaneous writers.
    /// </param>
    /// <returns>A new tracker instance with the requested contention profile.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    ///     Thrown when <paramref name="concurrency" /> is not a declared enum value.
    /// </exception>
    [PublicAPI]
    public static WatermarkTracker Create(WatermarkTrackerConcurrency concurrency = WatermarkTrackerConcurrency.Low) =>
        concurrency switch
        {
            WatermarkTrackerConcurrency.None => new SingleSlotWatermarkTracker(),
            WatermarkTrackerConcurrency.Low => new ConcurrencyWatermarkTracker(8),
            WatermarkTrackerConcurrency.Extreme => new ConcurrencyWatermarkTracker(16),
            _ => throw new ArgumentOutOfRangeException(nameof(concurrency), concurrency, null)
        };

    /// <summary>
    ///     A single atomic slot: minimal allocation, best suited for exactly one writer (or a writer plus several
    ///     readers) where no cross-thread CAS contention is expected.
    /// </summary>
    private sealed class SingleSlotWatermarkTracker : WatermarkTracker
    {
        private long _convergentProgress = Watermark.NothingValue;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void SetProgress(Watermark watermark)
        {
            if (watermark.IsSpecial)
                return;
            
            AdvancedInterlocked.CompareExchangeMax(ref _convergentProgress, watermark);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override Watermark GetProgress() =>
            Watermark.From(Volatile.Read(ref _convergentProgress));
    }

    /// <summary>
    ///     A sharded max: writers are hashed onto <c>slotCount</c> cache-line-aligned slots so concurrent
    ///     updates do not contend on a single memory location; readers reduce the slots in a tree.
    /// </summary>
    private sealed class ConcurrencyWatermarkTracker(int slotCount) : WatermarkTracker
    {
        private MonotonicMax _convergentProgress = new(slotCount, Watermark.NothingValue);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void SetProgress(Watermark watermark)
        {
            if (watermark.IsSpecial)
                return;
            
            _convergentProgress.Update(watermark);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override Watermark GetProgress() =>
            Watermark.From(_convergentProgress.Read());
    }
}

/// <summary>
///     The expected write contention of a <see cref="WatermarkTracker" />, selecting its internal strategy.
/// </summary>
[PublicAPI]
public enum WatermarkTrackerConcurrency
{
    /// <summary>
    ///     A single writer (the common case): one atomic slot, no sharding.
    /// </summary>
    None = 0,
    
    /// <summary>
    ///     A few concurrent writers: a sharded max over 8 cache-line-aligned slots.
    /// </summary>
    Low = 8,
    
    /// <summary>
    ///     Many concurrent writers: a sharded max over 16 cache-line-aligned slots.
    /// </summary>
    Extreme = 16
}