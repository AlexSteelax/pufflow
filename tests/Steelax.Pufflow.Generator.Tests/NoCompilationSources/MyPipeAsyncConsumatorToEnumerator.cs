using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
public partial class MyPipeAsyncConsumatorToEnumerator<T1, T2>
{
    public System.Collections.Generic.IEnumerator<T2> GetEnumerator(IAsyncConsumator<T1> source, Steelax.Pufflow.FlowContext ctx)
    {
        throw new System.NotImplementedException();
    }
}
