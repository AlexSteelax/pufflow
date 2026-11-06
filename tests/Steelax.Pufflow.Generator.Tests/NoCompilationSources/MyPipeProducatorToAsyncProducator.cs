using Steelax.Pufflow;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
internal partial class MyPipeProducatorToAsyncProducator<T1, T2>
{
    public MyPipeProducatorToAsyncProducator(out IProducator<T1> producer, IAsyncProducator<T2> writer, FlowContext _)
    {
        _ = writer;
        producer = null!;
    }
}