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
///     A pure function that projects the underlying value of a watermarked element into a new value of
///     <typeparamref name="TTarget" />. The <see cref="Watermark" /> is passed by input only so a selector can take
///     it into account while computing the result; the wrapping operator always preserves the watermark as-is.
/// </summary>
/// <typeparam name="TSource">The input value element type.</typeparam>
/// <typeparam name="TTarget">The output value element type.</typeparam>
/// <param name="source">The underlying value to project.</param>
/// <param name="watermark">The monotonic watermark attached to the element; may be <see cref="Watermark.Nothing()" />.</param>
/// <remarks>
///     When a projection must also rewrite (advance, clear, …) the watermark, do not use this delegate — instead
///     apply <see cref="MapSelector{TSource,TTarget}" /> to an element type that already carries the watermark as
///     part of its payload, so the new watermark becomes part of the returned <typeparamref name="TTarget" />.
/// </remarks>
[PublicAPI]
public delegate TTarget WatermarkedMapSelector<TSource, out TTarget>(scoped in TSource source, scoped in Watermark watermark);

/// <summary>
///     A pure function as <see cref="WatermarkedMapSelector{TSource,TTarget}" />, additionally receiving fixed
///     per-pipe <typeparamref name="TArgs" />.
/// </summary>
/// <typeparam name="TSource">The input value element type.</typeparam>
/// <typeparam name="TArgs">The type of the fixed arguments handed to the selector on every invocation.</typeparam>
/// <typeparam name="TTarget">The output value element type.</typeparam>
/// <param name="source">The underlying value to project.</param>
/// <param name="watermark">The monotonic watermark attached to the element; may be <see cref="Watermark.Nothing()" />.</param>
/// <param name="args">The fixed arguments captured once for the whole pipe, instead of a closure.</param>
/// <remarks>
///     As with the single-form watermarked selector, the watermark is never rewritten here: a projection that must
///     change the watermark works on a payload-carrying element through <see cref="MapSelector{TSource,TArgs,TTarget}" />.
/// </remarks>
[PublicAPI]
public delegate TTarget WatermarkedMapSelector<TSource, TArgs, out TTarget>(scoped in TSource source, scoped in Watermark watermark, scoped in TArgs args);

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