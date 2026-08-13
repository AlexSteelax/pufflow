using System.Collections.Generic;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
public partial class MyPipeEnumeratorToAsyncEnumerator<T1, T2>
{
    public void Fuse(in IEnumerator<T1> source, out IAsyncEnumerator<T2> target, Steelax.Pufflow.FlowContext ctx)
    {
        target = null!;
    }
}
