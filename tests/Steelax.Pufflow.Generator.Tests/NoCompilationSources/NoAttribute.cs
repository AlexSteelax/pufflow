using Steelax.Pufflow;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

internal partial class NoAttribute
{
    public NoAttribute(IConsumator<int> reader, FlowContext _)
    {
        _ = reader;
    }
}