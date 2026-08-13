using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Tests.Flows;

[Flow]
public partial class FlowPipeAsyncConsumatorToEnumerator<T1, T2>
{
    /// <remarks>
    ///     Тянет данные из source и отдает объект для вытягивания данных
    /// </remarks>
    public IEnumerator<T2> GetEnumerator(IAsyncConsumator<T1> source, FlowContext context)
    {
        throw new NotImplementedException();
    }
}