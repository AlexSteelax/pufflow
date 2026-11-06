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
    ///     are delivered in order, <c>WaitToReadAsync</c>/<c>IsCompleted</c> reflect the stream state, and end-of-stream
    ///     is signalled by the upstream completion. The interface is valid while the owning pipeline is alive.
    /// </param>
    [PublicAPI]
    public static void Expose<T>(this Fuse<IAsyncConsumator<T>> left, out IAsyncConsumator<T> source)
    {
        IAsyncConsumator<T>? captured = null;
        left.End(ExposeProcessor<T>.Configure(value => captured = value));
        source = captured!;
    }
}