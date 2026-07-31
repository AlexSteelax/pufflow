using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Tests.Flows;

[Flow]
public partial class FlowPipeEnumeratorToAsyncConsumator<T1, T2>
{
    /// <remarks>
    /// Тянет данные из source и отдает объект для вытягивания данных
    /// </remarks>
    public IAsyncConsumator<T2> GetAsyncConsumator(IEnumerator<T1> source, FlowContext context)
    {
        throw new NotImplementedException();
    }
}