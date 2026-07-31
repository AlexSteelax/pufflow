using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow;

public static partial class FlowExt
{
    /// <summary>
    /// Attaches a flowable source component to a <see cref="FlowSource"/>, creating a <see cref="Source{T}"/> marker.
    /// </summary>
    /// <typeparam name="T">The poll output type (e.g., <see cref="IConsumator{T}"/> or <c>IEnumerator{T}</c>).</typeparam>
    /// <param name="flow">The source component marked as <see cref="IFlowable{T}"/>.</param>
    /// <param name="source">The <see cref="FlowSource"/> providing cancellation for the pipeline.</param>
    /// <returns>A <see cref="Source{T}"/> marker ready for chaining.</returns>
    [PublicAPI]
    public static Source<T> Attach<T>(this IFlowable<Source<T>> flow, FlowSource source)
    {
        return new Source<T>(flow, source.Context);
    }

    /// <summary>
    /// Attaches a flowable source component with an explicit sync/async mode tag to a <see cref="FlowSource"/>.
    /// </summary>
    /// <typeparam name="Tk">A <see cref="Sync"/> or <see cref="Async"/> marker.</typeparam>
    /// <typeparam name="T">The poll output type (e.g., <see cref="IConsumator{T}"/> or <c>IEnumerator{T}</c>).</typeparam>
    /// <param name="flow">The source component marked as <see cref="IFlowable{T}"/>.</param>
    /// <param name="source">The <see cref="FlowSource"/> providing cancellation for the pipeline.</param>
    /// <returns>A <see cref="Source{TKind,T}"/> marker ready for chaining.</returns>
    [PublicAPI]
    public static Source<Tk, T> Attach<Tk, T>(this IFlowable<Source<Tk, T>> flow, FlowSource source)
    {
        return new Source<Tk, T>(flow, source.Context);
    }
}