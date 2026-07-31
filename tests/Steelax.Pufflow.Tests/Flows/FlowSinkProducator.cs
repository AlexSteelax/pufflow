using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Tests.Flows;

[Flow]
public partial class FlowSinkProducator<T>
{
    /// <remarks>
    /// Отдает объект для толкания данных в этот блок
    /// </remarks>
    public IProducator<T> GetProducator(FlowContext context)
    {
        throw new NotImplementedException();
    }
}