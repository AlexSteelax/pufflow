using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
public partial class MyCompositeSync<T1, T2>
{
    public void Fuse(out IProducator<T1> input, out IConsumator<T2> output, Steelax.Pufflow.FlowContext ctx)
    {
        input = null!;
        output = null!;
    }
}
