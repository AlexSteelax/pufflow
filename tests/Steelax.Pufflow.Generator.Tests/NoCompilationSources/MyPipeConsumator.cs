using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
public partial class MyPipeConsumator<T1, T2>
{
    public IConsumator<T2> GetConsumator(System.Collections.Generic.IEnumerator<T1> source, Steelax.Pufflow.FlowContext ctx)
    {
        throw new System.NotImplementedException();
    }
}
