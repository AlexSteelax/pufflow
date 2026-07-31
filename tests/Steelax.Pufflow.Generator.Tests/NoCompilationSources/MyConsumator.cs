using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
public partial class MyConsumator<T>
{
    public IConsumator<T> Handle(Steelax.Pufflow.FlowContext ctx)
    {
        throw new System.NotImplementedException();
    }
}
