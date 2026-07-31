using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
public partial class MyPipeEnumeratorToAsyncConsumator<T1, T2>
{
    public IAsyncConsumator<T2> GetAsyncConsumator(System.Collections.Generic.IEnumerator<T1> source, Steelax.Pufflow.FlowContext ctx)
    {
        throw new System.NotImplementedException();
    }
}
