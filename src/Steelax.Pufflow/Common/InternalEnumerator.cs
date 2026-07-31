namespace Steelax.Pufflow.Common;

internal readonly struct InternalEnumerator<T, TEnumerator>(TEnumerator enumerator)
    where TEnumerator : IEnumerator<T>
{
    public TEnumerator GetEnumerator() => enumerator;
}