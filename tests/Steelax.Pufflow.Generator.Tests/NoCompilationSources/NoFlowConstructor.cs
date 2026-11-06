using Steelax.Pufflow;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
internal partial class NoFlowConstructor
{
    public NoFlowConstructor(int value)
    {
        _ = value;
    }
}