using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Tests.Flows;

[Flow]
public partial class FlowPipeAsyncProducatorToAsyncEnumerator<T1, T2>
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
    public IAsyncEnumerator<T2> GetAsyncEnumerator(FlowContext context)
    {
        throw new NotImplementedException();
    }
}