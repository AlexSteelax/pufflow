using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
public partial class MyPipeEnumToAsyncProducator<T1, T2>
{
    public void Fuse(System.Collections.Generic.IEnumerator<T1> source, IAsyncProducator<T2> target, Steelax.Pufflow.FlowContext ctx)
    {
        throw new System.NotImplementedException();
    }
}
