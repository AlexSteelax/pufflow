using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Tests.Flows;

[Flow]
public partial class FlowPipeAsyncEnumeratorToEnumerator<T1, T2>
{
    public void Fuse(in IAsyncEnumerator<T1> source, out IEnumerator<T2> target, FlowContext context)
    {
        throw new NotImplementedException();
    }
}