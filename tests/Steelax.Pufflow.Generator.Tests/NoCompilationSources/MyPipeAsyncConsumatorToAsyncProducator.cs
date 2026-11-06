using Steelax.Pufflow;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
internal partial class MyPipeAsyncConsumatorToAsyncProducator<T1, T2>
{
    public MyPipeAsyncConsumatorToAsyncProducator(IAsyncConsumator<T1> reader, IAsyncProducator<T2> writer, FlowContext _)
    {
        _ = reader;
        _ = writer;
    }
}