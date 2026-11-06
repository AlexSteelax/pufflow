using Steelax.Pufflow;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
internal partial class MyPipeAsyncProducatorToConsumator<T1, T2>
{
    public MyPipeAsyncProducatorToConsumator(out IAsyncProducator<T1> producer, out IConsumator<T2> output, FlowContext _)
    {
        producer = null!;
        output = null!;
    }
}