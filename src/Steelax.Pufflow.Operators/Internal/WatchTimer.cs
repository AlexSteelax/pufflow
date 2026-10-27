using System.Runtime.CompilerServices;

namespace Steelax.Pufflow.Operators.Internal;

/// <summary>
///     A reusable recurring timer that invokes a callback against a fixed piece of state. The timer is
///     created stopped; <c>Start()</c> begins a recurring schedule (a first fire after the supplied
///     due time, then every <c>period</c> thereafter) and <c>Stop()</c> cancels it. The callback
///     observes a stable, immutable <typeparamref name="TState" /> (for example the owning processor or one
///     of its <c>readonly</c> fields), so the state is captured once and does not change.
/// </summary>
/// <typeparam name="TState">The type of the state captured by the timer callback.</typeparam>
public readonly struct WatchTimer<TState> : IDisposable, IAsyncDisposable
{
    private readonly ITimer? _timer;
    private readonly TimeSpan _period;

    /// <summary>
    ///     Initializes a timer with the recurring period recorded for the parameterless <see cref="Start()" />.
    ///     The timer is created stopped; firing begins only after <see cref="Start()" />/<see cref="Start(TimeSpan)" />.
    /// </summary>
    /// <param name="callback">The callback invoked on each timer fire.</param>
    /// <param name="state">The immutable state handed to the callback on every fire.</param>
    /// <param name="period">The recurring interval used by the parameterless <see cref="Start()" />.</param>
    /// <param name="timeProvider">The time source for the underlying timer; defaults to the system one.</param>
    public WatchTimer(Action<TState> callback,
        TState state,
        TimeSpan period,
        TimeProvider? timeProvider = null)
    {
        _period = period;
        _timer = (timeProvider ?? TimeProvider.System)
            .CreateTimer(
                callback: static o => Unsafe.As<object, Dispatcher>(ref o!).Fire(),
                state: new Dispatcher(state, callback),
                dueTime: Timeout.InfiniteTimeSpan,
                period: Timeout.InfiniteTimeSpan);
    }

    /// <summary>
    ///     Initializes a timer without a built-in period: it can only be started with an explicit due time via
    ///     <see cref="Start(TimeSpan)" />. The timer is created stopped; firing begins only after a start call.
    /// </summary>
    /// <param name="callback">The callback invoked on each timer fire.</param>
    /// <param name="state">The immutable state handed to the callback on every fire.</param>
    /// <param name="timeProvider">The time source for the underlying timer; defaults to the system one.</param>
    public WatchTimer(Action<TState> callback,
        TState state,
        TimeProvider? timeProvider = null) : this(callback, state,
        Timeout.InfiniteTimeSpan, timeProvider)
    {
    }
    
    /// <summary>
    ///     Cancels the recurring schedule; the callback will not fire again until <c>Start</c>.
    /// </summary>
    public void Stop() => _timer?.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);

    /// <summary>
    ///     Starts (or restarts) the recurring schedule: the first callback fires after <paramref name="dueTime" />,
    ///     then every <c>period</c> thereafter. Pass <c>period</c> as <paramref name="dueTime" /> to begin on the
    ///     regular cadence; <see cref="Stop" /> pauses it.
    /// </summary>
    public void Start(TimeSpan dueTime) => _timer?.Change(dueTime, _period);

    /// <summary>
    ///     Starts (or restarts) the recurring schedule on the constructor-supplied period: the first callback
    ///     fires after the period, then every period thereafter. <see cref="Stop" /> pauses it.
    /// </summary>
    public void Start() => _timer?.Change(_period, _period);

    private sealed class Dispatcher(TState state, Action<TState> callback)
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Fire() => callback.Invoke(state);
    }

    /// <summary>Stops the schedule and releases the underlying timer synchronously.</summary>
    public void Dispose()
    {
        Stop();
        _timer?.Dispose();
    }

    /// <summary>Stops the schedule and releases the underlying timer asynchronously.</summary>
    public ValueTask DisposeAsync()
    {
        Stop();
        return _timer?.DisposeAsync() ?? ValueTask.CompletedTask;
    }
}