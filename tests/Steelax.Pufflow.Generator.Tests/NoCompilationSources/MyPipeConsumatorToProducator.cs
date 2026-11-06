using Steelax.Pufflow;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
internal partial class MyPipeConsumatorToProducator<T1, T2>
{
    public MyPipeConsumatorToProducator(IConsumator<T1> reader, IProducator<T2> writer, FlowContext _)
    {
        _ = reader;
        _ = writer;
    }
}