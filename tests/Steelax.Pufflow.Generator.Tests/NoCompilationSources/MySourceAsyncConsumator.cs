using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
public partial class MySourceAsyncConsumator<T>
{
    public IAsyncConsumator<T> GetAsyncConsumator(Steelax.Pufflow.FlowContext ctx)
    {
        throw new System.NotImplementedException();
    }
}
