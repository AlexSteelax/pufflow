using Steelax.Pufflow;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
internal partial class MySinkAsyncProducator<T>
{
    public MySinkAsyncProducator(out IAsyncProducator<T> writer, FlowContext _)
    {
        writer = null!;
    }
}