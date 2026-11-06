using Steelax.Pufflow;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
internal partial class MyPipeAsyncConsumatorToProducator<T1, T2>
{
    public MyPipeAsyncConsumatorToProducator(IAsyncConsumator<T1> reader, IProducator<T2> writer, FlowContext _)
    {
        _ = reader;
        _ = writer;
    }
}