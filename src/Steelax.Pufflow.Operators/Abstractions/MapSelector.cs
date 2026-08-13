namespace Steelax.Pufflow.Operators.Abstractions;

/// <summary>
///     A pure function that projects an element of <typeparamref name="TSource" /> into an element of
///     <typeparamref name="TTarget" />.
/// </summary>
/// <typeparam name="TSource">The input element type.</typeparam>
/// <typeparam name="TTarget">The output element type.</typeparam>
/// <remarks>
///     The delegate is the shared projection primitive used across the operators: it drives the
///     <c>Map()</c> transform (a 1:1 element projection through a push stream) and selects the warming key
///     for the <c>Warming()</c> operator. <typeparamref name="TTarget" /> is covariant
///     (<see langword="out" />), so a selector producing a derived type can be used where a selector
///     producing a base type is expected.
/// </remarks>
[PublicAPI]
public delegate TTarget MapSelector<TSource, out TTarget>(scoped in TSource source);
