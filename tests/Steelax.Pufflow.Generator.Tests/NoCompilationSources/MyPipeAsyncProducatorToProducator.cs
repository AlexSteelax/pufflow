using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
public partial class MyPipeAsyncProducatorToProducator<T1, T2>
{
    public void Fuse(IAsyncProducator<T1> source, IProducator<T2> target, Steelax.Pufflow.FlowContext ctx)
    {
        throw new System.NotImplementedException();
    }
}
