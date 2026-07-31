using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Tests.Flows;

[Flow]
public partial class FlowPipeAsyncProducatorToAsyncConsumator<T1, T2>
{
    /// <remarks>
    /// Объект для проталкивания данных в этот блок
    /// </remarks>
    public IAsyncProducator<T1> GetAsyncProducator(FlowContext context)
    {
        throw new NotImplementedException();
    }

    /// <remarks>
    /// Объект для вытягивания данных из этого блока
    /// </remarks>
    public IAsyncConsumator<T2> GetAsyncConsumator(FlowContext context)
    {
        throw new NotImplementedException();
    }
}