using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow;

public static partial class FlowExt
{
    [PublicAPI]
    public static Sink<Async, IAsyncEnumerator<T>> Next<T>(this Source<IAsyncEnumerator<T>> left, IFlowable<Sink<Async, IAsyncEnumerator<T>>> right)
    {
        var leftEnumerator = FlowMarshal.GetAsyncEnumerator(left.Instance, left.Context);
        Debug.Assert(leftEnumerator is not null);

        var rightExecute = FlowMarshal.GetExecuteAsync(right, left.Context, leftEnumerator);
        Debug.Assert(rightExecute is not null);

        return new Sink<Async, IAsyncEnumerator<T>>(rightExecute, left.Context);
    }

    [PublicAPI]
    public static Task ExecuteAsync<T>(this Sink<Async, IAsyncEnumerator<T>> left)
    {
        var leftExecute = FlowMarshal.GetExecuteAsync(left.Instance, left.Context);
        Debug.Assert(leftExecute is not null);

        return (Task)((Func<object>)leftExecute).Invoke();
    }
}
