using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Tests.Flows;

[Flow]
public partial class FlowPipeEnumeratorToEnumerator<T1, T2>
{
    public void Fuse(in IEnumerator<T1> source, out IEnumerator<T2> target, FlowContext context)
    {
        throw new NotImplementedException();
    }
}