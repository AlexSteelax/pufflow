using Steelax.Pufflow.Operators.Aggregators;

namespace Steelax.Pufflow.Operators;

public static partial class OperatorExtensions
{
    /// <summary>
    ///     Terminates the pipeline and exposes the upstream pull side externally: the stream is not consumed
    ///     by another stage, but handed out as an <see cref="IAsyncConsumator{T}" /> that the caller can read
    ///     directly or feed into another pipeline (e.g. <see cref="MergeSources{T}" />).
    /// </summary>
    /// <typeparam name="T">The element type flowing out of the pipeline.</typeparam>
    /// <param name="left">The upstream pull source whose readable interface is exposed.</param>
    /// <param name="source">
    ///     Receives the exposed pull interface. Reading from it mirrors consuming the pipeline output: values
    ///     are delivered in order, <c>WaitToReadAsync</c>/<c>IsCompleted</c> reflect the stream state, and end-of-
    ///     stream is signalled by the upstream completion. The interface is valid while the owning pipeline
    ///     (the returned <see cref="Sink{T}" /> / its flow) is alive.
    /// </param>
    /// <returns>A sink terminating the current pipeline.</returns>
    /// <remarks>
    ///     Unlike a consuming <c>sink</c>, <see cref="Expose{T}" /> does not drain the stream itself — it
    ///     hands the pull interface to the caller and stops the stage chain. Lifetime ownership stays with the
    ///     flow that owns this sink; disposing that flow finalizes the exposed source.
    /// </remarks>
    [PublicAPI]
    public static Sink<IAsyncConsumator<T>> Expose<T>(
        this Source<IAsyncConsumator<T>> left,
        out IAsyncConsumator<T> source)
    {
        var processor = new ExposeProcessor<T>();
        var flow = left.End(processor);

        source = processor.Source;
        
        return flow;
    }
}