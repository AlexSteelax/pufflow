namespace Steelax.Pufflow.Common;

internal readonly struct InternalAsyncEnumerator<T, TAsyncEnumerator>(TAsyncEnumerator enumerator)
    where TAsyncEnumerator : IAsyncEnumerator<T>
{
    public TAsyncEnumerator GetAsyncEnumerator(FlowContext _) => enumerator;
}