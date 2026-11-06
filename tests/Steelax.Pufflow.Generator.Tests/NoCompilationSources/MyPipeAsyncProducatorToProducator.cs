using Steelax.Pufflow;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
internal partial class MyPipeAsyncProducatorToProducator<T1, T2>
{
    public MyPipeAsyncProducatorToProducator(out IAsyncProducator<T1> producer, IProducator<T2> writer, FlowContext _)
    {
        _ = writer;
        producer = null!;
    }
}