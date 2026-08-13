using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
public partial class MyConsumator<T>
{
    public void Fuse(out IConsumator<T> source, Steelax.Pufflow.FlowContext ctx)
    {
        source = null!;
    }
}
