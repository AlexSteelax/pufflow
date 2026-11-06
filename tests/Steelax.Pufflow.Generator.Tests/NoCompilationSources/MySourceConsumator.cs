using Steelax.Pufflow;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
internal partial class MySourceConsumator<T>
{
    public MySourceConsumator(out IConsumator<T> output, FlowContext _)
    {
        output = null!;
    }
}