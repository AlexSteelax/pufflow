namespace Steelax.Pufflow.Operators.Common;

/// <summary>
///     Associates a value with an optional <see cref="Watermark" />.
/// </summary>
/// <typeparam name="T">The type of the underlying value.</typeparam>
/// <remarks>
///     <see cref="Watermarked{T}" /> is a value-type wrapper carrying a value and the watermark
///     that was attached to it at the source. A watermark of <see cref="Watermark.Nothing()" />
///     (i.e. <see cref="IsNothing" /> is <c>true</c>) represents a value without an attached watermark.
///     <para />
///     Implicit conversion from <typeparamref name="T" /> produces a <see cref="Watermarked{T}" />
///     whose watermark is <see cref="Watermark.Nothing()" />; implicit conversion to
///     <typeparamref name="T" /> unwraps the underlying value.
/// </remarks>
[PublicAPI]
public readonly struct Watermarked<T> : IEquatable<Watermarked<T>>
{
    private readonly Watermark _watermark;

    /// <summary>
    ///     Initializes a new <see cref="Watermarked{T}" /> with the specified value and watermark.
    /// </summary>
    /// <param name="value">The underlying value.</param>
    /// <param name="watermark">The attached watermark; use <see cref="Watermark.Nothing()" /> for none.</param>
    [PublicAPI]
    public Watermarked(T value, Watermark watermark)
    {
        Value = value;
        _watermark = watermark;
    }

    /// <summary>
    ///     Gets the underlying value.
    /// </summary>
    [PublicAPI]
    public T Value { get; }

    /// <summary>
    ///     Gets the attached watermark.
    /// </summary>
    [PublicAPI]
    public Watermark Watermark => _watermark;

    /// <summary>
    ///     Indicates whether no watermark is attached (i.e. <see cref="Watermark" /> is
    ///     <see cref="Watermark.Nothing()" />).
    /// </summary>
    [PublicAPI]
    public bool IsNothing => _watermark.IsNothing;

    // -- Conversions ---------------------------------------------------------

    /// <summary>
    ///     Implicitly converts a raw value into a <see cref="Watermarked{T}" /> without a watermark.
    /// </summary>
    /// <param name="value">The value to wrap.</param>
    [PublicAPI]
    public static implicit operator Watermarked<T>(T value)
    {
        return new Watermarked<T>(value, Watermark.Nothing());
    }

    /// <summary>
    ///     Implicitly unwraps the underlying value from a <see cref="Watermarked{T}" />.
    /// </summary>
    /// <param name="source">The watermarked value.</param>
    [PublicAPI]
    public static implicit operator T(Watermarked<T> source)
    {
        return source.Value;
    }

    // -- Equality ------------------------------------------------------------

    /// <summary>
    ///     Indicates whether the current watermarked value equals another one (by value and watermark).
    /// </summary>
    /// <param name="other">The other watermarked value to compare with.</param>
    /// <returns><see langword="true" /> when both the value and the watermark are equal.</returns>
    [PublicAPI]
    public bool Equals(Watermarked<T> other)
    {
        return _watermark == other._watermark && EqualityComparer<T>.Default.Equals(Value, other.Value);
    }

    /// <summary>
    ///     Indicates whether the current watermarked value equals another object.
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>
    ///     <see langword="true" /> when <paramref name="obj" /> is a <see cref="Watermarked{T}" /> with an equal value
    ///     and watermark.
    /// </returns>
    [PublicAPI]
    public override bool Equals(object? obj)
    {
        return obj is Watermarked<T> other && Equals(other);
    }

    /// <summary>
    ///     Returns a hash code for this watermarked value.
    /// </summary>
    /// <returns>A hash code combining the watermark and the underlying value.</returns>
    [PublicAPI]
    public override int GetHashCode()
    {
        return HashCode.Combine(_watermark, Value);
    }

    /// <summary>
    ///     Indicates whether two watermarked values are equal.
    /// </summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns><see langword="true" /> when the values are equal.</returns>
    public static bool operator ==(Watermarked<T> left, Watermarked<T> right)
    {
        return left.Equals(right);
    }

    /// <summary>
    ///     Indicates whether two watermarked values are not equal.
    /// </summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns><see langword="true" /> when the values are not equal.</returns>
    public static bool operator !=(Watermarked<T> left, Watermarked<T> right)
    {
        return !left.Equals(right);
    }

    /// <summary>
    ///     Deconstructs the value into its value and watermark components.
    /// </summary>
    /// <param name="value">The underlying value.</param>
    /// <param name="watermark">The attached watermark.</param>
    [PublicAPI]
    public void Deconstruct(out T value, out Watermark watermark)
    {
        value = Value;
        watermark = _watermark;
    }
}