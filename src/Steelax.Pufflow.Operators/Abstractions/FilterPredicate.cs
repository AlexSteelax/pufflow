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

/// <summary>
///     An internal implementation seam for filtering a <see cref="Carrier{T}" /> stream. It decides on the whole
///     envelope rather than a plain value, so progress-only elements can be forwarded or dropped by explicit policy.
///     Stateless processors consume this shape; static adapters lift a value-level <see cref="FilterPredicate{TSource}" />
///     into it.
/// </summary>
/// <typeparam name="TSource">The carried value element type.</typeparam>
/// <typeparam name="TScope">The type of the operator-held scope state.</typeparam>
/// <typeparam name="TArgs">The type of the fixed per-pipe arguments.</typeparam>
/// <param name="source">The carrier envelope to test.</param>
/// <param name="scope">The operator-held scope state.</param>
/// <param name="args">The fixed per-pipe arguments.</param>
/// <returns><see langword="true" /> to forward <paramref name="source" />, otherwise to drop it.</returns>
internal delegate bool CarrierFilter<TSource, in TScope, TArgs>(scoped in Carrier<TSource> source, TScope scope, scoped in TArgs args);