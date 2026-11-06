using Steelax.Pufflow;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
internal partial class MyPipeProducatorToAsyncConsumator<T1, T2>
{
    public MyPipeProducatorToAsyncConsumator(out IProducator<T1> producer, out IAsyncConsumator<T2> output, FlowContext _)
    {
        producer = null!;
        output = null!;
    }
}