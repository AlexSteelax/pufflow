using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Abstractions;

/// <summary>
///     A pure function that projects an element of <typeparamref name="TSource" /> into an element of
///     <typeparamref name="TTarget" />.
/// </summary>
/// <typeparam name="TSource">The input element type.</typeparam>
/// <typeparam name="TTarget">The output element type.</typeparam>
/// <param name="source">The element to project.</param>
/// <remarks>
///     The delegate is the shared projection primitive used across the operators: it drives the
///     <c>Map()</c> transform (a 1:1 element projection through a push stream) and selects the warming key
///     for the <c>Warming()</c> operator. <typeparamref name="TTarget" /> is covariant
///     (<see langword="out" />), so a selector producing a derived type can be used where a selector
///     producing a base type is expected.
/// </remarks>
[PublicAPI]
public delegate TTarget MapSelector<TSource, out TTarget>(scoped in TSource source);

/// <summary>
///     A pure function that projects an element of <typeparamref name="TSource" /> into an element of
///     <typeparamref name="TTarget" />, given fixed per-pipe <typeparamref name="TArgs" />.
/// </summary>
/// <typeparam name="TSource">The input element type.</typeparam>
/// <typeparam name="TArgs">The type of the fixed arguments handed to the selector on every invocation.</typeparam>
/// <typeparam name="TTarget">The output element type.</typeparam>
/// <param name="source">The element to project.</param>
/// <param name="args">The fixed arguments captured once for the whole pipe, instead of a closure.</param>
/// <remarks>
///     An argument-bearing selector fires a per-flow state parameter rather than closing over a value, so the
///     projection can stay a cached, statically-known method and be inlined into a hot loop.
/// </remarks>
[PublicAPI]
public delegate TTarget MapSelector<TSource, TArgs, out TTarget>(scoped in TSource source, scoped in TArgs args);

/// <summary>
///     An internal implementation seam that folds the public selector arities into one delegate driving the
///     stateless mapping processor (<c>BypassMapProcessor</c>): <c>scope</c> is the operator-held state (the public
///     selector itself for single- and argument-bearing mapping) and <c>args</c> are the fixed per-pipe arguments.
///     Consumers pass an inlined static adapter (e.g. <c>MapSelector</c> method group) here; the delegate is not part
///     of the public API.
/// </summary>
/// <typeparam name="TSource">The input element type.</typeparam>
/// <typeparam name="TScope">The type of the operator-held scope state.</typeparam>
/// <typeparam name="TArgs">The type of the fixed per-pipe arguments.</typeparam>
/// <typeparam name="TTarget">The output element type.</typeparam>
/// <param name="source">The element to project.</param>
/// <param name="scope">The operator-held scope state.</param>
/// <param name="args">The fixed per-pipe arguments.</param>
/// <returns>The projected element.</returns>
internal delegate TTarget MapSelector<TSource, in TScope, TArgs, out TTarget>(scoped in TSource source, TScope scope, scoped in TArgs args);

/// <summary>
///     An internal implementation seam over the carrier payload: it drives a stateless mapping processor for a
///     <see cref="Carrier{T}" /> stream. Unlike the public <see cref="MapSelector{TSource,TTarget}" /> which maps a
///     plain value, this delegate operates on the whole envelope. When the caller prefers value-only projection, a
///     static adapter lifts such a selector into this form and decides the "no data" (bare progress) case itself, so
///     the processor never sees Carrier nested inside any further layer.
/// </summary>
/// <typeparam name="TSource">The carried value element type.</typeparam>
/// <typeparam name="TScope">The type of the operator-held scope state.</typeparam>
/// <typeparam name="TArgs">The type of the fixed per-pipe arguments.</typeparam>
/// <typeparam name="TTarget">The produced value element type.</typeparam>
/// <param name="source">The carrier envelope to project.</param>
/// <param name="scope">The operator-held scope state.</param>
/// <param name="args">The fixed per-pipe arguments.</param>
/// <returns>The projected carrier envelope.</returns>
internal delegate Carrier<TTarget> CarrierMapSelector<TSource, in TScope, TArgs, TTarget>(scoped in Carrier<TSource> source, TScope scope, scoped in TArgs args);