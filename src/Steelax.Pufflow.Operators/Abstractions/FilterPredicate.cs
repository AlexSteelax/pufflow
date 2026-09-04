using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Abstractions;

/// <summary>
///     A pure predicate deciding whether an element of <typeparamref name="TSource" /> passes a filter.
/// </summary>
/// <typeparam name="TSource">The input element type.</typeparam>
/// <param name="source">The element to evaluate.</param>
/// <returns>
///     <see langword="true" /> to keep (forward) the element, <see langword="false" /> to drop it.
/// </returns>
[PublicAPI]
public delegate bool FilterPredicate<TSource>(scoped in TSource source);

/// <summary>
///     A pure predicate deciding whether an element of <typeparamref name="TSource" /> passes a filter,
///     given fixed per-pipe <typeparamref name="TArgs" />.
/// </summary>
/// <typeparam name="TSource">The input element type.</typeparam>
/// <typeparam name="TArgs">The type of the fixed arguments handed to the predicate on every invocation.</typeparam>
/// <param name="source">The element to evaluate.</param>
/// <param name="args">The fixed arguments captured once for the whole pipe, instead of a closure.</param>
/// <returns>
///     <see langword="true" /> to keep (forward) the element, <see langword="false" /> to drop it.
/// </returns>
[PublicAPI]
public delegate bool FilterPredicate<TSource, TArgs>(scoped in TSource source, scoped in TArgs args);

/// <summary>
///     A pure predicate deciding, for a <see cref="Watermarked{T}" /> element, whether its underlying value of
///     <typeparamref name="TSource" /> passes a filter. The <see cref="Watermark" /> is passed by input only so a
///     predicate can take it into account; it is never changed here — the wrapping operator always preserves it.
/// </summary>
/// <typeparam name="TSource">The input value element type.</typeparam>
/// <param name="source">The underlying value to evaluate.</param>
/// <param name="watermark">The monotonic watermark attached to the element; may be <see cref="Watermark.Nothing()" />.</param>
/// <returns>
///     <see langword="true" /> to keep (forward) the value, <see langword="false" /> to turn it into a bare
///     progress marker.
/// </returns>
[PublicAPI]
public delegate bool WatermarkedFilterPredicate<TSource>(scoped in TSource source, scoped in Watermark watermark);

/// <summary>
///     A pure predicate as overloaded <see cref="WatermarkedFilterPredicate{TSource}" />, additionally receiving
///     fixed per-pipe <typeparamref name="TArgs" />.
/// </summary>
/// <typeparam name="TSource">The input value element type.</typeparam>
/// <typeparam name="TArgs">The type of the fixed arguments handed to the predicate on every invocation.</typeparam>
/// <param name="source">The underlying value to evaluate.</param>
/// <param name="watermark">The monotonic watermark attached to the element; may be <see cref="Watermark.Nothing()" />.</param>
/// <param name="args">The fixed arguments captured once for the whole pipe, instead of a closure.</param>
/// <returns>
///     <see langword="true" /> to keep (forward) the value, <see langword="false" /> to turn it into a bare
///     progress marker.
/// </returns>
[PublicAPI]
public delegate bool WatermarkedFilterPredicate<TSource, TArgs>(scoped in TSource source, scoped in Watermark watermark, scoped in TArgs args);

/// <summary>
///     An internal implementation seam that unifies the public predicate arities into one delegate consumed by a
///     stateless processing pipe (<c>BypassFilterProcessor</c>) whose <c>scope</c> is the operator-held state and
///     <c>args</c> are the fixed per-pipe arguments. Consumers pass an inlined static adapter such as
///     <c>FilterHandler</c> here; this delegate is not part of the public API.
/// </summary>
/// <typeparam name="TSource">The input element type.</typeparam>
/// <typeparam name="TScope">The type of the operator-held scope state (for example the public predicate itself).</typeparam>
/// <typeparam name="TArgs">The type of the fixed per-pipe arguments.</typeparam>
/// <param name="source">The element to evaluate.</param>
/// <param name="scope">The operator-held scope state.</param>
/// <param name="args">The fixed per-pipe arguments.</param>
/// <returns>
///     <see langword="true" /> to keep (forward) the element, <see langword="false" /> to drop it.
/// </returns>
internal delegate bool FilterPredicate<TSource, in TScope, TArgs>(scoped in TSource source, TScope scope, scoped in TArgs args);