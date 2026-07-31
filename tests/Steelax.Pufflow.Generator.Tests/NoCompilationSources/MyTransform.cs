using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
public partial class MyTransform<T1, T2>
{
    public System.Collections.Generic.IEnumerator<T2> GetEnumerator(
        System.Collections.Generic.IEnumerator<T1> source,
        Steelax.Pufflow.FlowContext ctx)
    {
        yield break;
    }
}
