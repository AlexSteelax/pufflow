using System.Collections.Generic;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

public partial class NoAttribute
{
    public void Fuse(out IEnumerator<int> source, Steelax.Pufflow.FlowContext ctx)
    {
        source = null!;
    }
}
