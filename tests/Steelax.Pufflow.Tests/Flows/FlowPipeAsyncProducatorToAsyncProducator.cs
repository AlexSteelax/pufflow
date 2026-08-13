using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Tests.Flows;

[Flow]
public partial class FlowPipeAsyncProducatorToAsyncProducator<T1, T2>
{
    /// <remarks>
    ///     Отдает объект для проталкивания данных в этот блок и принимает объект для проталкивания данных в следующий блок
    /// </remarks>
    public IAsyncProducator<T1> GetAsyncProducator(IAsyncProducator<T2> target, FlowContext context)
    {
        throw new NotImplementedException();
    }
}