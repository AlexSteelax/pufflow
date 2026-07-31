namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
public partial class MyPipeEnumeratorToAsyncEnumerator<T1, T2>
{
    public System.Collections.Generic.IAsyncEnumerator<T2> GetAsyncEnumerator(System.Collections.Generic.IEnumerator<T1> source, Steelax.Pufflow.FlowContext ctx)
    {
        throw new System.NotImplementedException();
    }
}
