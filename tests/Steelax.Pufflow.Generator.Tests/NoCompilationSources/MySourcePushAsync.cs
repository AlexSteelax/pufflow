using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
public partial class MySourcePushAsync<T>
{
    public System.Threading.Tasks.Task ExecuteAsync(IAsyncProducator<T> target, Steelax.Pufflow.FlowContext ctx)
    {
        throw new System.NotImplementedException();
    }
}
