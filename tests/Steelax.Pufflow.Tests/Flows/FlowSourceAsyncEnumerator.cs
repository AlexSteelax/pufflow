using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Tests.Flows;

[Flow]
public partial class FlowSourceAsyncEnumerator<T>(IEnumerable<T> items)
{
    /// <remarks>
    /// Отдает объект для вытягивания данных
    /// </remarks>
    public async IAsyncEnumerator<T> GetAsyncEnumerator(FlowContext context)
    {
        await Task.Yield();
        
        foreach (var item in items)
            yield return item;
    }
}