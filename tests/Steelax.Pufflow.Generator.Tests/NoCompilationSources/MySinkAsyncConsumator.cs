using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
public partial class MySinkAsyncConsumator<T>
{
    public System.Threading.Tasks.Task ExecuteAsync(IAsyncConsumator<T> source, Steelax.Pufflow.FlowContext ctx)
    {
        throw new System.NotImplementedException();
    }
}
