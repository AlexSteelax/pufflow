using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
public partial class MyPipeAsyncConsumatorToAsyncProducator<T1, T2>
{
    public System.Threading.Tasks.Task ExecuteAsync(IAsyncConsumator<T1> source, IAsyncProducator<T2> target, Steelax.Pufflow.FlowContext ctx)
    {
        throw new System.NotImplementedException();
    }
}
