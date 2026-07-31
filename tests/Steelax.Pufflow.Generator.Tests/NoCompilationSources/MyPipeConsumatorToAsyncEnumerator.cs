using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
public partial class MyPipeConsumatorToAsyncEnumerator<T1, T2>
{
    public System.Collections.Generic.IAsyncEnumerator<T2> GetAsyncEnumerator(IConsumator<T1> source, Steelax.Pufflow.FlowContext ctx)
    {
        throw new System.NotImplementedException();
    }
}
