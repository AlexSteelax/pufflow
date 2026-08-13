namespace Steelax.Pufflow.Abstractions;

/// <summary>
///     Marks a component as a dataflow block with a statically-known flow shape.
/// </summary>
/// <typeparam name="TFlow">
///     A marker struct (<see cref="Source{T}" />, <see cref="Sink{T}" />, or <see cref="Pipe{TLeft, TRight}" />)
///     that encodes the component's role in the pipeline at compile time.
/// </typeparam>
/// <remarks>
///     This interface is implemented automatically by the source generator
///     when a class is annotated with the <see cref="FlowAttribute" />.
///     It enables type-safe chaining via <c>FlowExt.Next</c> extension methods
///     by capturing the exact source/sink/pipe shape in the generic parameter.
/// </remarks>
public interface IFlowable<TFlow> where TFlow : struct;