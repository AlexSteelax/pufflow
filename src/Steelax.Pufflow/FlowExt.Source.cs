using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow;

public static partial class FlowExt
{
    /// <summary>
    ///     Opens a flow on this <see cref="FlowSource" /> from a flowable source component,
    ///     producing a <see cref="Source{T}" /> marker ready for chaining.
    /// </summary>
    /// <typeparam name="T">The poll output type (e.g., <see cref="IConsumator{T}" /> or <c>IEnumerator{T}</c>).</typeparam>
    /// <param name="source">The <see cref="FlowSource" /> providing cancellation for the pipeline.</param>
    /// <param name="flow">The source component marked as <see cref="IFlowable{T}" />.</param>
    /// <returns>A <see cref="Source{T}" /> marker ready for chaining.</returns>
    [PublicAPI]
    public static Source<T> On<T>(this FlowSource source, IFlowable<Source<T>> flow)
    {
        var context = source.Context;
        var meta = FlowMetaNode.Create(flow, FlowKind.None, typeof(T).Classify());
        return new Source<T>(meta, context);
    }
}
