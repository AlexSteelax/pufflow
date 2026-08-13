using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
public partial class MySourceAsyncConsumator<T>
{
    public void Fuse(out IAsyncConsumator<T> source, Steelax.Pufflow.FlowContext ctx)
    {
        source = null!;
    }
}
