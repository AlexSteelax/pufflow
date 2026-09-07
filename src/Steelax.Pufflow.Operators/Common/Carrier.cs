using System.Runtime.CompilerServices;

namespace Steelax.Pufflow.Operators.Common;

/// <summary>
///     A single envelope flowing through a stream stage: an optional value accompanied by an optional watermark.
///     Replaces nesting wrappers such as <c>Watermarked&lt;Unio&lt;TValue, Unit&gt;&gt;</c>.
/// </summary>
/// <typeparam name="T">The element (data) type carried by this envelope.</typeparam>
/// <remarks>
    ///     <para>
    ///         An envelope either carries data (<see cref="HasValue" /> is <see langword="true" />) or is a bare
    ///         progress marker that still advances the stream but has no payload of its own. The previous modelling
    ///         of that empty-notion required an outer watermark wrapper with a union branch (<c>Unio&lt;…&gt;</c>) and
    ///         a bare <c>Unit</c> arm; that forced consumers to pattern-match over growing nested unions as operators
    ///         were combined.
    ///     </para>
///     <para>
///         Here the option is folded into a single struct, so an operator can map <c>Carrier&lt;T&gt;</c> to
///         <c>Carrier&lt;U&gt;</c> without touching the watermark shape. A watermark of
///         <see cref="Watermark.Nothing()" /> means none was attached; the currently attached value is available via
///         <see cref="Watermark" />.
///     </para>
/// </remarks>
[PublicAPI]
public readonly struct Carrier<T>
{
    /// <summary>The carried values when <see cref="HasValue" /> is <see langword="true" />; a readonly default slot otherwise.</summary>
    private readonly T _value;

    /// <summary>The watermark attached to this element.</summary>
    private readonly Watermark _watermark;

    /// <summary>Initializes a new data-carrying envelope.</summary>
    /// <param name="value">The underlying value.</param>
    /// <param name="watermark">
    ///     The attached watermark, or <see cref="Watermark.Nothing()" /> when the value has none.
    /// </param>
    public Carrier(T value, Watermark watermark)
    {
        _value = value;
        _watermark = watermark;
        HasValue = true;
    }

    /// <summary>Initializes a bare progress marker with no data. Only its watermark matters.</summary>
    /// <param name="watermark">
    ///     The attached watermark; may be <see cref="Watermark.Nothing()" /> for an unattached marker.
    /// </param>
    public Carrier(Watermark watermark)
    {
        _value = default!;
        _watermark = watermark;
        HasValue = false;
    }

    /// <summary>
    ///     Gets the underlying value. Only valid when <see cref="HasValue" /> is <see langword="true" /> (a
    ///     data-carrying envelope); reading it on a bare progress marker throws
    ///     <see cref="InvalidOperationException" />.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    ///     Thrown when the envelope is a bare progress marker (no data).
    /// </exception>
    public T Value
    {
        get
        {
            if (!HasValue)
                throw new InvalidOperationException("Cannot read the value of an empty (bare progress) carrier.");

            return _value;
        }
    }

    /// <summary>Gets the attached watermark. May be <see cref="Watermark.Nothing()" />.</summary>
    public Watermark Watermark => _watermark;

    /// <summary>
    ///     <see langword="true" /> when this envelope carries data; <see langword="false" /> for a bare progress
    ///     marker.
    /// </summary>
    public bool HasValue { get; }

    /// <summary>
    ///     <see langword="true" /> when a real (non-<see cref="Watermark.Nothing()" />) watermark is attached.
    /// </summary>
    public bool HasWatermark => _watermark.IsNothing == false;

    /// <summary>Implicitly converts a bare value into a data-carrying envelope without a watermark.</summary>
    /// <param name="value">The value to wrap.</param>
    public static implicit operator Carrier<T>(T value) => new(value, Watermark.Nothing());

    /// <summary>Deconstructs the envelope into its value and watermark parts.</summary>
    /// <param name="value">The underlying value (default when <see cref="HasValue" /> is <see langword="false" />).</param>
    /// <param name="watermark">The attached watermark; <see cref="Watermark.Nothing()" /> when none.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Deconstruct(out T value, out Watermark watermark)
    {
        value = _value;
        watermark = _watermark;
    }
}
