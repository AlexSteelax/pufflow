using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
public partial class MyPipeAsyncConsumatorToAsyncEnumerator<T1, T2>
{
    public System.Collections.Generic.IAsyncEnumerator<T2> GetAsyncEnumerator(IAsyncConsumator<T1> source, Steelax.Pufflow.FlowContext ctx)
    {
        throw new System.NotImplementedException();
    }
}
