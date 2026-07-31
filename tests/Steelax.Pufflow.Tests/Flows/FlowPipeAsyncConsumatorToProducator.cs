using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Tests.Flows;

[Flow]
public partial class FlowPipeAsyncConsumatorToProducator<T1, T2>
    // IFlowable<Pipe<Task, IAsyncConsumator<T1>, IProducator<T2>>>
{
    /// <remarks>
    /// Тянет данные из source и толкает данные в target
    /// </remarks>
    public Task ExecuteAsync(IAsyncConsumator<T1> source, IProducator<T2> target, FlowContext context)
    {
        throw new NotImplementedException();
    }
}