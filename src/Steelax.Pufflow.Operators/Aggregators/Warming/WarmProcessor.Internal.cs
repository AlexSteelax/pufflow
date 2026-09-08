using Steelax.Pufflow.Operators.Abstractions;
using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Operators.Internal;
using Steelax.Toolkit.HighPerformance.Concurrency.Primitives;

namespace Steelax.Pufflow.Operators.Aggregators.Warming;

internal sealed partial class WarmProcessor<TKey, TValue, TGroup, TWarm>
{
    #region signals
    
    /// <summary>The fan-in slot signaled by the <see cref="Warmer{TKey,TWarm}" /> when work completes.</summary>
    private const int WarmSlot = 1;

    /// <summary>The fan-in slot signaled when the wrapped output producer frees capacity (<see cref="_writer" />).</summary>
    private const int OutputSlot = 2;

    /// <summary>The fan-in slot signaled when the input source has data (<see cref="_source" />).</summary>
    private const int InputSlot = 3;

    /// <summary>The fan-in slot used only to wake a sleeping loop on cancellation (the source bridge uses slot 31).</summary>
    private const int CancellationSlot = 0;

    /// <summary>The fan-in slot signaled periodically by the watchdog timer to re-check a sleeping loop.</summary>
    private const int WatchdogSlot = 4;

    /// <summary>The fan-in slot signaled when the one-shot linger timer fires to seal a partial tail.</summary>
    private const int LingerSlot = 5;
    
    #endregion
    
    #region source and sink
    
    /// <summary>The wrapped upstream source; assigned in <see cref="Fuse" />.</summary>
    private ManualConsumator<Carrier<TValue>> _source = null!;

    /// <summary>The wrapped downstream producer; assigned in <see cref="Fuse" />.</summary>
    private ManualProducator<Carrier<Unio<TValue, TGroup>>> _writer = null!;
    
    #endregion
    
    #region state
    
    /// <summary>Creates the per-key accumulator buffers.</summary>
    private readonly IWarmAccumulatorFactory<TKey, TValue, TGroup> _accumulatorFactory;

    /// <summary>The per-key delayed buffers (the FlowGate dictionary).</summary>
    private readonly Dictionary<TKey, WarmAccumulator<TValue, TGroup>> _delayedQueue;
    
    /// <summary>The shared fan-in multiplexing source readiness and warm completion.</summary>
    private readonly FanInSlim _fanIn;

    /// <summary>Selects the warming key for a value.</summary>
    private readonly MapSelector<TValue, TKey> _keySelector;
    
    /// <summary>Decides whether a key requires warming and receives the warm result for a key.</summary>
    private readonly IWarmPolicy<TKey, TWarm> _policy;

    /// <summary>The maximum total weight the per-key delayed buffers may hold (the buffer budget limit).</summary>
    private readonly long _queueWeightLimit;
    
    /// <summary>The warmer providing key segmentation, concurrent warming, and the watermark barrier.</summary>
    private readonly Warmer<TKey, TWarm> _warmer;

    /// <summary>The started periodic watchdog; created in the constructor and stopped when the loop exits.</summary>
    private readonly WatchTimer<FanInSlim> _watchdog;
    
    /// <summary>The idle interval after which a partially filled warming tail is sealed (via <c>SealTail</c>).</summary>
    private readonly TimeSpan _segmentTtl;

    /// <summary>
    ///     One-shot linger timer that wakes the loop (through <see cref="LingerSlot" />) to seal a partially
    ///     filled tail after <see cref="_segmentTtl" /> of idleness. Created in the constructor; re-armed
    ///     directly (<c>_linger.Start(_segmentLinger)</c>) while the source is silent.
    /// </summary>
    private readonly WatchTimer<FanInSlim> _ttl;
    
    /// <summary>The global accumulated weight currently held by the per-key buffers (the buffer budget).</summary>
    private long _totalWeight;
    
    #endregion
}