using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
public partial class MyPipeAsyncConsumatorToAsyncConsumator<T1, T2>
{
    public IAsyncConsumator<T2> GetAsyncConsumator(IAsyncConsumator<T1> source, Steelax.Pufflow.FlowContext ctx)
    {
        throw new System.NotImplementedException();
    }
}
