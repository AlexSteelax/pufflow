using System.Collections.Generic;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
public partial class MySource<T>
{
    public void Fuse(out IEnumerator<T> source, Steelax.Pufflow.FlowContext ctx)
    {
        source = null!;
    }
}
