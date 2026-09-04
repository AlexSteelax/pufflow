using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
public partial class MyPipeProducatorToAsyncConsumator<T1, T2>
{
    public void Fuse(out IProducator<T1> input, out IAsyncConsumator<T2> output, Steelax.Pufflow.FlowContext ctx)
    {
        input = null!;
        output = null!;
    }
}
