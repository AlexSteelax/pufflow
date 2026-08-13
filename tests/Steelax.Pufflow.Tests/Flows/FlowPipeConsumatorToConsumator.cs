using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Tests.Flows;

[Flow]
public partial class FlowPipeConsumatorToConsumator<T1, T2>
{
    public void Fuse(in IConsumator<T1> source, out IConsumator<T2> target, FlowContext context)
    {
        throw new NotImplementedException();
    }
}