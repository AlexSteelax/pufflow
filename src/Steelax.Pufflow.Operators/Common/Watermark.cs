namespace Steelax.Pufflow.Operators.Common;

/// <summary>
///     Represents a monotonic timestamp based on <see cref="Environment.TickCount64" />.
/// </summary>
/// <remarks>
///     <see cref="Watermark" /> is a value-type wrapper over <see cref="Environment.TickCount64" />
///     — a monotonically increasing millisecond counter measured from system startup.
///     Unlike <see cref="DateTimeOffset" />, it is immune to system clock adjustments
///     and DST transitions, making it suitable for watermark tracking in streaming pipelines.
///     <para />
///     Use <see cref="Nothing" /> to represent the absence of a watermark.
    ///     <para />
    ///     The value is stored with a constant bias of <c>+1</c>: the sentinel <c>0</c> (which is also
    ///     <c>default(Watermark)</c>) means "no watermark", while all real ticks are strictly positive.
    ///     This keeps <see cref="Nothing" /> equal to the default instance without sacrificing
    ///     <see cref="From" />(0) as a legitimate tick value.
    /// </remarks>
[PublicAPI]
public readonly struct Watermark : IEquatable<Watermark>, IComparable<Watermark>
{
    /// <summary>
    ///     Internal bias applied to stored values so that <c>0</c> (the default) is reserved for <see cref="Nothing" />.
    /// </summary>
    private const long Bias = 1;

    private readonly long _value;

    private Watermark(long value)
    {
        _value = value;
    }

    /// <summary>
    ///     Sentinel value representing "no watermark". Equal to <c>-1</c> in the logical value space.
    ///     Thanks to the internal bias, <c>default(Watermark)</c> is equal to <see cref="Nothing" /> as well.
    /// </summary>
    [PublicAPI] public const long NothingValue = -1;

    /// <summary>
    ///     Returns a new <see cref="Watermark" /> advanced by the specified <paramref name="interval" />.
    /// </summary>
    /// <param name="interval">The time interval to add. Can be negative.</param>
    /// <returns>A new watermark shifted by <paramref name="interval" />.</returns>
    /// <remarks>
    ///     Uses saturating arithmetic to prevent overflow when the interval is extreme.
    /// </remarks>
    [PublicAPI]
    public Watermark Add(TimeSpan interval)
    {
        return IsNothing
            ? new Watermark(Biased(long.CreateSaturating(interval.TotalMilliseconds)))
            : new Watermark(_value + long.CreateSaturating(interval.TotalMilliseconds));
    }

    /// <summary>
    ///     Creates a <see cref="Watermark" /> from the current <see cref="Environment.TickCount64" /> value.
    /// </summary>
    /// <returns>A watermark representing the current monotonic time.</returns>
    [PublicAPI]
    public static Watermark FromEnvironmentTicks()
    {
        return new Watermark(Environment.TickCount64);
    }

    /// <summary>
    ///     Creates a <see cref="Watermark" /> from a raw <see cref="long" /> value.
    /// </summary>
    /// <param name="value">The raw tick value. <c>0</c> is a legitimate tick distinct from <see cref="Nothing" />.</param>
    /// <returns>A watermark wrapping <paramref name="value" />.</returns>
    [PublicAPI]
    public static Watermark From(long value)
    {
        return new Watermark(Biased(value));
    }

    private static long Biased(long value)
    {
        return value == long.MaxValue ? long.MaxValue : value + Bias;
    }

    /// <summary>
    ///     Returns the sentinel watermark representing "no watermark".
    /// </summary>
    /// <returns>A watermark whose <see cref="IsNothing" /> is <c>true</c>. Also equal to <c>default(Watermark)</c>.</returns>
    [PublicAPI]
    public static readonly Watermark Nothing = new(Biased(NothingValue));

    /// <summary>
    ///     Indicates whether this watermark lies below the <see cref="Nothing" /> sentinel in the
    ///     logical value space — a reserved range for special sentinels (e.g. completion markers).
    /// </summary>
    [PublicAPI]
    public bool IsSpecial => _value < 0;

    /// <summary>
    ///     Indicates whether this watermark is the sentinel "nothing" value.
    /// </summary>
    [PublicAPI]
    public bool IsNothing => _value == Biased(NothingValue);
    
    /// <inheritdoc/>
    public override string ToString() => (_value - Bias).ToString();

    // -- Comparison operators ------------------------------------------------

    /// <summary>
    ///     Indicates whether the left watermark is earlier than the right one.
    /// </summary>
    /// <param name="w1">The left operand.</param>
    /// <param name="w2">The right operand.</param>
    /// <returns><see langword="true" /> when <paramref name="w1" /> is earlier than <paramref name="w2" />.</returns>
    public static bool operator <(Watermark w1, Watermark w2) => w1._value < w2._value;
    
    /// <inheritdoc cref="Watermark.op_LessThan(Watermark, Watermark)"/>
    public static bool operator <(Watermark w1, long w2) => w1._value < w2;

    /// <summary>
    ///     Indicates whether the left watermark is later than the right one.
    /// </summary>
    /// <param name="w1">The left operand.</param>
    /// <param name="w2">The right operand.</param>
    /// <returns><see langword="true" /> when <paramref name="w1" /> is later than <paramref name="w2" />.</returns>
    public static bool operator >(Watermark w1, Watermark w2) => w1._value > w2._value;
    
    /// <inheritdoc cref="Watermark.op_GreaterThan(Watermark, Watermark)"/>
    public static bool operator >(Watermark w1, long w2) => w1._value > w2;

    /// <summary>
    ///     Indicates whether the left watermark is not later than the right one.
    /// </summary>
    /// <param name="w1">The left operand.</param>
    /// <param name="w2">The right operand.</param>
    /// <returns><see langword="true" /> when <paramref name="w1" /> is earlier than or equal to <paramref name="w2" />.</returns>
    public static bool operator <=(Watermark w1, Watermark w2) => w1._value <= w2._value;
    
    /// <inheritdoc cref="Watermark.op_LessThanOrEqual(Watermark, Watermark)"/>
    public static bool operator <=(Watermark w1, long w2) => w1._value <= w2;

    /// <summary>
    ///     Indicates whether the left watermark is not earlier than the right one.
    /// </summary>
    /// <param name="w1">The left operand.</param>
    /// <param name="w2">The right operand.</param>
    /// <returns><see langword="true" /> when <paramref name="w1" /> is later than or equal to <paramref name="w2" />.</returns>
    public static bool operator >=(Watermark w1, Watermark w2) => w1._value >= w2._value;
    
    /// <inheritdoc cref="Watermark.op_GreaterThanOrEqual(Watermark, Watermark)"/>
    public static bool operator >=(Watermark w1, long w2) => w1._value >= w2;

    // -- Equality ------------------------------------------------------------

    /// <summary>
    ///     Indicates whether two watermarks are equal.
    /// </summary>
    /// <param name="w1">The left operand.</param>
    /// <param name="w2">The right operand.</param>
    /// <returns><see langword="true" /> when the watermarks hold the same tick value.</returns>
    public static bool operator ==(Watermark w1, Watermark w2) => w1._value == w2._value;
    
    /// <inheritdoc cref="Watermark.op_Equality(Watermark, Watermark)"/>
    public static bool operator ==(Watermark w1, long w2) => w1._value == w2;

    /// <summary>
    ///     Indicates whether two watermarks are not equal.
    /// </summary>
    /// <param name="w1">The left operand.</param>
    /// <param name="w2">The right operand.</param>
    /// <returns><see langword="true" /> when the watermarks hold different tick values.</returns>
    public static bool operator !=(Watermark w1, Watermark w2) => w1._value != w2._value;
    
    /// <inheritdoc cref="Watermark.op_Inequality(Watermark, Watermark)"/>
    public static bool operator !=(Watermark w1, long w2) => w1._value != w2;

    /// <summary>
    ///     Indicates whether the current watermark equals another one.
    /// </summary>
    /// <param name="other">The other watermark to compare with.</param>
    /// <returns><see langword="true" /> when both hold the same tick value.</returns>
    public bool Equals(Watermark other) => _value == other._value;

    /// <summary>
    ///     Compares the current watermark with another one.
    /// </summary>
    /// <param name="other">The other watermark to compare with.</param>
    /// <returns>A value indicating the relative order of the two watermarks.</returns>
    public int CompareTo(Watermark other) => _value.CompareTo(other._value);

    /// <summary>
    ///     Indicates whether the current watermark equals another object.
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns><see langword="true" /> when <paramref name="obj" /> is a <see cref="Watermark" /> with the same tick value.</returns>
    public override bool Equals(object? obj) => obj is Watermark other && Equals(other);

    /// <summary>
    ///     Returns a hash code for this watermark.
    /// </summary>
    /// <returns>A hash code derived from the underlying tick value.</returns>
    public override int GetHashCode() => _value.GetHashCode();

    /// <summary>
    ///     Implicitly converts a <see cref="Watermark" /> to its underlying <see cref="long" /> value.
    /// </summary>
    /// <param name="w">The watermark to convert.</param>
    public static implicit operator long(Watermark w) => w._value - Bias;
}