using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Tests.Flows;

[Flow]
public partial class FlowPipeAsyncConsumatorToEnumerator<T1, T2>
{
    public void Fuse(in IAsyncConsumator<T1> source, out IEnumerator<T2> target, FlowContext context)
    {
        throw new NotImplementedException();
    }
}