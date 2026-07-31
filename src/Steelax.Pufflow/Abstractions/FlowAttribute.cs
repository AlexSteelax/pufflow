namespace Steelax.Pufflow.Abstractions;

/// <summary>
/// Marks a class as a dataflow pipeline component.
/// </summary>
/// <remarks>
/// When a class is annotated with this attribute, the <c>GetFlowGenerator</c>
/// source generator analyzes its public handler methods
/// (<c>GetEnumerator</c>, <c>GetAsyncEnumerator</c>, <c>Handle</c>,
/// <c>GetConsumator</c>, <c>GetAsyncConsumator</c>, <c>GetProducator</c>,
/// <c>GetAsyncProducator</c>, <c>Execute</c>, <c>ExecuteAsync</c>)
/// and generates an <see cref="IFlowable{T}"/> implementation that captures
/// the component's role (source, sink, or pipe) at compile time.
/// </remarks>
[AttributeUsage(AttributeTargets.Class)]
public sealed class FlowAttribute : Attribute;