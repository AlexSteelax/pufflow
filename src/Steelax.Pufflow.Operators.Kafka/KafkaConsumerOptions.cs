namespace Steelax.Pufflow.Operators.Kafka;

/// <summary>
///     Configuration options for <see cref="KafkaConsumerProcessor{TKey,TValue}" />.
/// </summary>
/// <remarks>
///     Options are added as needed. Currently present: the lifecycle interval, the emergency/idle ratios and
///     intervals, the window pool size and lifetime, the pending (emergency) deque capacity, and the offset
///     advance strategy.
/// </remarks>
[PublicAPI]
public sealed record KafkaConsumerOptions
{
    /// <summary>
    ///     Initializes options with the specified lifecycle interval.
    /// </summary>
    /// <param name="lifeCycleIntervalMs">The lifecycle interval in milliseconds. Must be greater than zero.</param>
    public KafkaConsumerOptions(int lifeCycleIntervalMs) : this(TimeSpan.FromMilliseconds(lifeCycleIntervalMs))
    {
    }

    /// <summary>
    ///     Initializes options with the specified lifecycle interval.
    /// </summary>
    public KafkaConsumerOptions(TimeSpan lifeCycleInterval)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(lifeCycleInterval.TotalMilliseconds);
        LifeCycleInterval = lifeCycleInterval;
        WatermarkAdvanceInterval = lifeCycleInterval;
    }

    /// <summary>Gets the lifecycle interval.</summary>
    [PublicAPI]
    public TimeSpan LifeCycleInterval
    {
        get;
    }

    /// <summary>
    ///     The fraction of the lifecycle interval that defines the emergency interval (0..1). Must be within
    ///     the range [0; 1].
    /// </summary>
    [PublicAPI]
    public required float EmergencyRatio
    {
        get;
        init
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, 0f, nameof(EmergencyRatio));
            ArgumentOutOfRangeException.ThrowIfGreaterThan(value, 1f, nameof(EmergencyRatio));
            field = value;
            EmergencyInterval = EmergencyRatio * LifeCycleInterval;
        }
    }

    /// <summary>
    ///     The advance-timer interval used in the emergency mode: computed from <see cref="EmergencyRatio" />
    ///     and <see cref="LifeCycleInterval" />. Read-only.
    /// </summary>
    [PublicAPI]
    public TimeSpan EmergencyInterval
    {
        get;
        private init;
    }

    /// <summary>
    ///     The fraction of the lifecycle interval that defines the idle interval (0..1). Must be within
    ///     the range [0; 1].
    /// </summary>
    [PublicAPI]
    public required float IdleRatio
    {
        get;
        init
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, 0f, nameof(IdleRatio));
            ArgumentOutOfRangeException.ThrowIfGreaterThan(value, 1f, nameof(IdleRatio));
            field = value;
            IdleInterval = IdleRatio * LifeCycleInterval;
        }
    }

    /// <summary>
    ///     The advance-timer interval used in the idle mode: computed from <see cref="IdleRatio" />
    ///     and <see cref="LifeCycleInterval" />. Read-only.
    /// </summary>
    [PublicAPI]
    public TimeSpan IdleInterval
    {
        get;
        private init;
    }

    /// <summary>
    ///     The period between the last emitted watermark and the current fresh one that must elapse before a bare
    ///     progress marker is pushed into the stream while the source is idle. Defaults to
    ///     <see cref="LifeCycleInterval" />.
    /// </summary>
    /// <remarks>
    ///     Periodic idle markers keep the stream's watermark alive when the source has no data — this matters when
    ///     several sources are merged into one stream: the merged watermark is the minimum of the sources, so a
    ///     single one-shot marker from a quiet source would freeze the merged watermark and block the other
    ///     sources' commits. Pushing a fresh watermark every <see cref="WatermarkAdvanceInterval" /> lets the
    ///     minimum keep advancing at the pace of the slowest source.
    /// </remarks>
    [PublicAPI]
    public TimeSpan WatermarkAdvanceInterval
    {
        get;
        init
        {
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(value, TimeSpan.Zero, nameof(WatermarkAdvanceInterval));
            field = value;
        }
    }

    /// <summary>
    ///     The maximum number of concurrently open progress windows (the window pool size).
    /// </summary>
    [PublicAPI]
    public required int WindowSize
    {
        get;
        init
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(value, nameof(WindowSize));
            field = value;
        }
    }

    /// <summary>
    ///     The window lifetime: a window is closed once this time elapses from its opening.
    /// </summary>
    [PublicAPI]
    public required TimeSpan WindowLifetime
    {
        get;
        init
        {
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(value, TimeSpan.Zero, nameof(WindowLifetime));
            field = value;
        }
    }

    /// <summary>
    ///     The offset advance strategy (OffsetStore / ManualCommit).
    /// </summary>
    [PublicAPI]
    public AdvanceStrategy AdvanceStrategy
    {
        get;
        init;
    }

    /// <summary>
    ///     The capacity of the pending (emergency) deque: the maximum number of records buffered when the
    ///     output producer cannot accept them.
    /// </summary>
    [PublicAPI]
    public required int EmergencyCapacity
    {
        get;
        init
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(value, nameof(EmergencyCapacity));
            field = value;
        }
    }
}
