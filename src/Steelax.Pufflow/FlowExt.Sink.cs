using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow;

public static partial class FlowExt
{
    /// <summary>
    ///     Connects an async enumerator source to an async sink, forming a terminal pipeline stage.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="left">The source emitting an <see cref="IAsyncEnumerator{T}" />.</param>
    /// <param name="right">
    ///     The sink component that accepts an <see cref="IAsyncEnumerator{T}" /> and executes the terminal
    ///     logic.
    /// </param>
    /// <returns>A <see cref="Sink{TKind,T}" /> marker representing the terminal stage.</returns>
    /// <remarks>
    ///     Internally resolves the left enumerator via <see cref="FlowMarshal.GetAsyncEnumerator" />,
    ///     then wraps the right component's <c>ExecuteAsync</c> method via <see cref="FlowMarshal.GetExecuteAsync" />.
    ///     The result is a callable sink that can be invoked via <see cref="ExecuteAsync{T}" />.
    /// </remarks>
    [PublicAPI]
    public static Sink<Async, IAsyncEnumerator<T>> Next<T>(this Source<IAsyncEnumerator<T>> left,
        IFlowable<Sink<Async, IAsyncEnumerator<T>>> right)
    {
        var leftEnumerator = FlowMarshal.GetAsyncEnumerator(left.Instance, left.Context);
        Debug.Assert(leftEnumerator is not null);

        var rightExecute = FlowMarshal.GetExecuteAsync(right, left.Context, leftEnumerator);
        Debug.Assert(rightExecute is not null);

        return new Sink<Async, IAsyncEnumerator<T>>(rightExecute, left.Context);
    }

    /// <summary>
    ///     Executes a terminal async sink, running the pipeline to completion.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="left">The sink marker returned from a previous <c>Next</c> call.</param>
    /// <returns>A <see cref="Task" /> that completes when the pipeline finishes.</returns>
    /// <remarks>
    ///     Invokes the wrapped <c>ExecuteAsync</c> delegate via <see cref="FlowMarshal.GetExecuteAsync" />
    ///     and casts the result to <see cref="Task" />.
    /// </remarks>
    [PublicAPI]
    public static Task ExecuteAsync<T>(this Sink<Async, IAsyncEnumerator<T>> left)
    {
        var leftExecute = FlowMarshal.GetExecuteAsync(left.Instance, left.Context);
        Debug.Assert(leftExecute is not null);

        return (Task)((Func<object>)leftExecute).Invoke();
    }
}