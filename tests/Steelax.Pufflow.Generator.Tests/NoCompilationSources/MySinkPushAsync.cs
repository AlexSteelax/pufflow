using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
public partial class MySinkPushAsync<T>
{
    public void Fuse(in IAsyncProducator<T> target, Steelax.Pufflow.FlowContext ctx)
    {
        throw new System.NotImplementedException();
    }
}
