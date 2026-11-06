using Steelax.Pufflow;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
internal partial class MyPipeConsumatorToAsyncProducator<T1, T2>
{
    public MyPipeConsumatorToAsyncProducator(IConsumator<T1> reader, IAsyncProducator<T2> writer, FlowContext _)
    {
        _ = reader;
        _ = writer;
    }
}