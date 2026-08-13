using System.Collections.Generic;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
public partial class MyAsyncSource<T>
{
    public void Fuse(out IAsyncEnumerator<T> source, Steelax.Pufflow.FlowContext ctx)
    {
        source = null!;
    }
}
