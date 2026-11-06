using Steelax.Pufflow;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
internal partial class MySinkProducator<T>
{
    public MySinkProducator(out IProducator<T> writer, FlowContext _)
    {
        writer = null!;
    }
}