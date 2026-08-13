using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Tests.Flows;

[Flow]
public partial class FlowPipeAsyncEnumeratorToAsyncEnumerator<T1, T2>(Func<T1, T2> transform)
{
    /// <remarks>
    ///     Тянет данные из source и отдает объект для вытягивания данных
    /// </remarks>
    public async IAsyncEnumerator<T2> GetAsyncEnumerator(IAsyncEnumerator<T1> source, FlowContext context)
    {
        while (!context.Token.IsCancellationRequested && await source.MoveNextAsync())
            yield return transform.Invoke(source.Current);
    }
}