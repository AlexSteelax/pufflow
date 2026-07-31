using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
public partial class MyPipeConsumatorToProducator<T1, T2>
{
    public void Execute(IConsumator<T1> source, IProducator<T2> target, Steelax.Pufflow.FlowContext ctx)
    {
        throw new System.NotImplementedException();
    }
    
    public System.Threading.Tasks.Task ExecuteAsync(IConsumator<T1> source, IProducator<T2> target, Steelax.Pufflow.FlowContext ctx)
    {
        throw new System.NotImplementedException();
    }
}
