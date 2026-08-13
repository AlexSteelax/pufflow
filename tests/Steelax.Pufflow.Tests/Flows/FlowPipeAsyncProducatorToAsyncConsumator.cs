using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Tests.Flows;

[Flow]
public partial class FlowPipeAsyncProducatorToAsyncConsumator<T1, T2>
{
    public void Fuse(out IAsyncProducator<T1> source, out IAsyncConsumator<T2> target, FlowContext context)
    {
        throw new NotImplementedException();
    }
}