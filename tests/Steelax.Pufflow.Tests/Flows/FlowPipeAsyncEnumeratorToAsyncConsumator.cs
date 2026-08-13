using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Tests.Flows;

[Flow]
public partial class FlowPipeAsyncEnumeratorToAsyncConsumator<T1, T2>
{
    /// <remarks>
    ///     Тянет данные из source и отдает объект для вытягивания данных
    /// </remarks>
    public IAsyncConsumator<T2> GetAsyncConsumator(IAsyncEnumerator<T1> source, FlowContext context)
    {
        throw new NotImplementedException();
    }
}