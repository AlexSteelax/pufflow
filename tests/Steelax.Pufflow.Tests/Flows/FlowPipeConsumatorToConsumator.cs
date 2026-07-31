using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Tests.Flows;

[Flow]
public partial class FlowPipeConsumatorToConsumator<T1, T2>
{
    /// <remarks>
    /// Тянет данные из source и отдает объект для вытягивания данных
    /// </remarks>
    public IConsumator<T2> GetConsumator(IConsumator<T1> source, FlowContext context)
    {
        throw new NotImplementedException();
    }
}