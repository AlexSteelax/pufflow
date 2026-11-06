namespace Steelax.Pufflow.Abstractions;

/// <summary>
///     Marks a class as a dataflow pipeline component.
/// </summary>
/// <remarks>
///     When a class is annotated with this attribute, the <c>GetFlowGenerator</c>
///     source generator analyzes its public instance constructor. The constructor's
///     flow interface parameters (in/out <c>IConsumator</c>/<c>IProducator</c> and their
///     async variants, plus a trailing <c>FlowContext</c>) determine the component's role
///     (source, sink, or pipe) and its pull/push direction. The generator emits a static
///     <c>Configure(...)</c> factory that builds the component lazily inside the chain.
/// </remarks>
[AttributeUsage(AttributeTargets.Class)]
public sealed class FlowAttribute : Attribute;