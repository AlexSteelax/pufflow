using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
public partial class MyAsyncSource<T>
{
    public System.Collections.Generic.IAsyncEnumerator<T> GetAsyncEnumerator(Steelax.Pufflow.FlowContext ctx)
    {
        throw new System.NotImplementedException();
    }
}