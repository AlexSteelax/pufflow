using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
public partial class MyPipeProducator<T1, T2>
{
    public void Fuse(in IProducator<T1> input, out IProducator<T2> output, Steelax.Pufflow.FlowContext ctx)
    {
        output = null!;
    }
}
