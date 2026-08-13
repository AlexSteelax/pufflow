using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Tests.Flows;

[Flow]
public partial class FlowPipeConsumatorToAsyncConsumator<T1, T2>
{
    public void Fuse(in IConsumator<T1> source, out IAsyncConsumator<T2> target, FlowContext context)
    {
        throw new NotImplementedException();
    }
}