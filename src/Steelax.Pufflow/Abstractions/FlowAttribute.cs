namespace Steelax.Pufflow.Abstractions;

/// <summary>
///     Marks a class as a dataflow pipeline component.
/// </summary>
/// <remarks>
///     When a class is annotated with this attribute, the <c>GetFlowGenerator</c>
///     source generator analyzes its public handler methods (the unified
///     <c>Fuse(...)</c> contract, whose <c>in</c>/<c>out</c> parameter modifiers
///     and flow interface families determine the role) and generates an
///     <see cref="IFlowable{T}" /> implementation that captures the component's
///     role (source, sink, or pipe) at compile time.
/// </remarks>
[AttributeUsage(AttributeTargets.Class)]
public sealed class FlowAttribute : Attribute;