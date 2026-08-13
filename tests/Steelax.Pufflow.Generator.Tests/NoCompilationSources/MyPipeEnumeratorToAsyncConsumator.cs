using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
public partial class MyPipeEnumeratorToAsyncConsumator<T1, T2>
{
    public void Fuse(in System.Collections.Generic.IEnumerator<T1> source, out IAsyncConsumator<T2> target, Steelax.Pufflow.FlowContext ctx)
    {
        target = null!;
    }
}
