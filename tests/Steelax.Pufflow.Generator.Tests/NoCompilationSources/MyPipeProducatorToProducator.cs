using Steelax.Pufflow;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
internal partial class MyPipeProducatorToProducator<T1, T2>
{
    public MyPipeProducatorToProducator(out IProducator<T1> producer, IProducator<T2> writer, FlowContext _)
    {
        _ = writer;
        producer = null!;
    }
}