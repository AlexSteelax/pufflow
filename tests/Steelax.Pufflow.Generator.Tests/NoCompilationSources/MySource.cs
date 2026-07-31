using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
public partial class MySource<T>
{
    public System.Collections.Generic.IEnumerator<T> GetEnumerator(Steelax.Pufflow.FlowContext ctx)
    {
        yield break;
    }
}