using Steelax.Pufflow;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
internal partial class MyPipeAsyncProducatorToAsyncConsumator<T1, T2>
{
    public MyPipeAsyncProducatorToAsyncConsumator(out IAsyncProducator<T1> producer, out IAsyncConsumator<T2> output, FlowContext _)
    {
        producer = null!;
        output = null!;
    }
}