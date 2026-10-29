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
}
