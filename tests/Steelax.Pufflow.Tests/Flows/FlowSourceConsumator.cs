using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Tests.Flows;

[Flow]
public partial class FlowSourceConsumator<T>
{
    /// <remarks>
    /// Отдает объект для вытягивания данных
    /// </remarks>
    public IConsumator<T> GetConsumator(FlowContext context)
    {
        throw new NotImplementedException();
    }
}