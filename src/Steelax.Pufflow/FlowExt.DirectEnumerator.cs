using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow;

public static partial class FlowExt
{
    /// <summary>
    /// Chains a synchronous enumerator source to a synchronous enumerator pipe.
    /// </summary>
    /// <typeparam name="T1">The input element type.</typeparam>
    /// <typeparam name="T2">The output element type.</typeparam>
    /// <param name="left">The source emitting <see cref="IEnumerator{T1}"/>.</param>
    /// <param name="rightFlow">The pipe transforming <see cref="IEnumerator{T1}"/> to <see cref="IEnumerator{T2}"/>.</param>
    /// <returns>A source emitting <see cref="IEnumerator{T2}"/>.</returns>
    /// <remarks>Not yet implemented.</remarks>
    [PublicAPI]
    public static Source<IEnumerator<T2>> Next<T1, T2>(this Source<IEnumerator<T1>> left, IFlowable<Pipe<IEnumerator<T1>, IEnumerator<T2>>> rightFlow)
    {
        throw new NotImplementedException();
    }

    /// <summary>
    /// Chains an async enumerator source to an async enumerator pipe.
    /// </summary>
    /// <typeparam name="T1">The input element type.</typeparam>
    /// <typeparam name="T2">The output element type.</typeparam>
    /// <param name="left">The source emitting <see cref="IAsyncEnumerator{T1}"/>.</param>
    /// <param name="right">The pipe transforming <see cref="IAsyncEnumerator{T1}"/> to <see cref="IAsyncEnumerator{T2}"/>.</param>
    /// <returns>A source emitting <see cref="IAsyncEnumerator{T2}"/>.</returns>
    /// <remarks>
    /// Internally resolves the left async enumerator and invokes the right component's
    /// <c>GetAsyncEnumerator</c> method via <see cref="FlowMarshal.GetAsyncEnumerator"/>.
    /// This is the primary working chain for async-to-async transformations.
    /// </remarks>
    [PublicAPI]
    public static Source<IAsyncEnumerator<T2>> Next<T1, T2>(this Source<IAsyncEnumerator<T1>> left, IFlowable<Pipe<IAsyncEnumerator<T1>, IAsyncEnumerator<T2>>> right)
    {
        var leftEnumerator = FlowMarshal.GetAsyncEnumerator(left.Instance, left.Context);
        Debug.Assert(leftEnumerator is not null);

        var rightEnumerator = FlowMarshal.GetAsyncEnumerator(right, left.Context, leftEnumerator);
        Debug.Assert(rightEnumerator is not null);

        return new Source<IAsyncEnumerator<T2>>(rightEnumerator, left.Context);
    }

    /// <summary>
    /// Chains a synchronous enumerator source to an async enumerator pipe (sync→async transition).
    /// </summary>
    /// <typeparam name="T1">The input element type.</typeparam>
    /// <typeparam name="T2">The output element type.</typeparam>
    /// <param name="left">The source emitting <see cref="IEnumerator{T1}"/>.</param>
    /// <param name="rightFlow">The pipe transforming <see cref="IEnumerator{T1}"/> to <see cref="IAsyncEnumerator{T2}"/>.</param>
    /// <returns>A source emitting <see cref="IAsyncEnumerator{T2}"/>.</returns>
    /// <remarks>Not yet implemented.</remarks>
    [PublicAPI]
    public static Source<IAsyncEnumerator<T2>> Next<T1, T2>(this Source<IEnumerator<T1>> left, IFlowable<Pipe<IEnumerator<T1>, IAsyncEnumerator<T2>>> rightFlow)
    {
        throw new NotImplementedException();
    }

    /// <summary>
    /// Chains a synchronous consumator source to a synchronous enumerator pipe.
    /// </summary>
    /// <typeparam name="T1">The input element type.</typeparam>
    /// <typeparam name="T2">The output element type.</typeparam>
    /// <param name="left">The source emitting <see cref="IConsumator{T1}"/>.</param>
    /// <param name="rightFlow">The pipe transforming <see cref="IConsumator{T1}"/> to <see cref="IEnumerator{T2}"/>.</param>
    /// <returns>A source emitting <see cref="IEnumerator{T2}"/>.</returns>
    /// <remarks>Not yet implemented.</remarks>
    [PublicAPI]
    public static Source<IEnumerator<T2>> Next<T1, T2>(this Source<IConsumator<T1>> left, IFlowable<Pipe<IConsumator<T1>, IEnumerator<T2>>> rightFlow)
    {
        throw new NotImplementedException();
    }

    /// <summary>
    /// Chains a synchronous consumator source to an async enumerator pipe (sync poll→async poll transition).
    /// </summary>
    /// <typeparam name="T1">The input element type.</typeparam>
    /// <typeparam name="T2">The output element type.</typeparam>
    /// <param name="left">The source emitting <see cref="IConsumator{T1}"/>.</param>
    /// <param name="rightFlow">The pipe transforming <see cref="IConsumator{T1}"/> to <see cref="IAsyncEnumerator{T2}"/>.</param>
    /// <returns>A source emitting <see cref="IAsyncEnumerator{T2}"/>.</returns>
    /// <remarks>Not yet implemented.</remarks>
    [PublicAPI]
    public static Source<IAsyncEnumerator<T2>> Next<T1, T2>(this Source<IConsumator<T1>> left, IFlowable<Pipe<IConsumator<T1>, IAsyncEnumerator<T2>>> rightFlow)
    {
        throw new NotImplementedException();
    }

    /// <summary>
    /// Chains an async consumator source to an async enumerator pipe.
    /// </summary>
    /// <typeparam name="T1">The input element type.</typeparam>
    /// <typeparam name="T2">The output element type.</typeparam>
    /// <param name="left">The source emitting <see cref="IAsyncConsumator{T1}"/>.</param>
    /// <param name="rightFlow">The pipe transforming <see cref="IAsyncConsumator{T1}"/> to <see cref="IAsyncEnumerator{T2}"/>.</param>
    /// <returns>A source emitting <see cref="IAsyncEnumerator{T2}"/>.</returns>
    /// <remarks>Not yet implemented.</remarks>
    [PublicAPI]
    public static Source<IAsyncEnumerator<T2>> Next<T1, T2>(this Source<IAsyncConsumator<T1>> left, IFlowable<Pipe<IAsyncConsumator<T1>, IAsyncEnumerator<T2>>> rightFlow)
    {
        throw new NotImplementedException();
    }
}
