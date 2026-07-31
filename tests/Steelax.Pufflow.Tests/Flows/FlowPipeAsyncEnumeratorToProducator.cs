using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Tests.Flows;

[Flow]
public partial class FlowPipeAsyncEnumeratorToProducator<T1, T2>
{
    /// <remarks>
    /// Тянет данные из source и толкает данные в target
    /// </remarks>
    public Task ExecuteAsync(IAsyncEnumerator<T1> source, IProducator<T2> target, FlowContext context)
    {
        throw new NotImplementedException();
    }
}