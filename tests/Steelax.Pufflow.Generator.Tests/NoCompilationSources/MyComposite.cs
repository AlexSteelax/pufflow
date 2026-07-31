using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
public partial class MyComposite<T1, T2>
{
    public IAsyncProducator<T1> GetAsyncProducator(Steelax.Pufflow.FlowContext ctx)
    {
        throw new System.NotImplementedException();
    }

    public IAsyncConsumator<T2> GetAsyncConsumator(Steelax.Pufflow.FlowContext ctx)
    {
        throw new System.NotImplementedException();
    }
}
