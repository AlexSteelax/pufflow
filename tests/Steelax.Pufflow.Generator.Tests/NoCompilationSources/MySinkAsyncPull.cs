using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
public partial class MySinkAsyncPull<T>
{
    public System.Threading.Tasks.Task ExecuteAsync(System.Collections.Generic.IAsyncEnumerator<T> source, Steelax.Pufflow.FlowContext ctx)
    {
        throw new System.NotImplementedException();
    }
}
