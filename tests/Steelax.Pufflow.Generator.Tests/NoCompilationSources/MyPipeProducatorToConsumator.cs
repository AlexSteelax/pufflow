using Steelax.Pufflow;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
internal partial class MyPipeProducatorToConsumator<T1, T2>
{
    public MyPipeProducatorToConsumator(out IProducator<T1> producer, out IConsumator<T2> output, FlowContext _)
    {
        producer = null!;
        output = null!;
    }
}