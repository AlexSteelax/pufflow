using Steelax.Pufflow;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
internal partial class MyPipeAsyncProducatorToAsyncProducator<T1, T2>
{
    public MyPipeAsyncProducatorToAsyncProducator(out IAsyncProducator<T1> producer, IAsyncProducator<T2> writer, FlowContext _)
    {
        _ = writer;
        producer = null!;
    }
}