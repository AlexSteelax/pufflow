using System.Collections.Generic;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
public partial class MyPipeAsyncEnumToAsyncConsumator<T1, T2>
{
    public void Fuse(out IAsyncEnumerator<T1> input, out IAsyncConsumator<T2> output, Steelax.Pufflow.FlowContext ctx)
    {
        input = null!;
        output = null!;
    }
}
