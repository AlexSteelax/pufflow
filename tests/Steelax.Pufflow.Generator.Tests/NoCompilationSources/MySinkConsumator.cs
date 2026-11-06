using Steelax.Pufflow;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
internal partial class MySinkConsumator<T>
{
    public MySinkConsumator(IConsumator<T> reader, FlowContext _)
    {
        _ = reader;
    }
}