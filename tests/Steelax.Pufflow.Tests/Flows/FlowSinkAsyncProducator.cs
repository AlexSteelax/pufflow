using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Tests.Flows;

[Flow]
public partial class FlowSinkAsyncProducator<T>
{
    /// <remarks>
    /// Отдает объект для толкания данных в этот блок
    /// </remarks>
    public IAsyncProducator<T> GetAsyncProducator(FlowContext context)
    {
        throw new NotImplementedException();
    }
}