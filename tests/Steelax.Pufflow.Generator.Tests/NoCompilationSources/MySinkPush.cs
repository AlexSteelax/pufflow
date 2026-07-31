using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
public partial class MySinkPush<T>
{
    public IProducator<T> GetProducator(Steelax.Pufflow.FlowContext ctx)
    {
        throw new System.NotImplementedException();
    }
}
