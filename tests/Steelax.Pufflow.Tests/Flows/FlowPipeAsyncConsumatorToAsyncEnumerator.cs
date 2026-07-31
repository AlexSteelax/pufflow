using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Tests.Flows;

[Flow]
public partial class FlowPipeAsyncConsumatorToAsyncEnumerator<T1, T2>
{
    /// <remarks>
    /// Тянет данные из source и отдает объект для вытягивания данных
    /// </remarks>
    public IAsyncEnumerator<T2> GetAsyncEnumerator(IAsyncConsumator<T1> source, FlowContext flowContext)
    {
        throw new NotImplementedException();
    }
}