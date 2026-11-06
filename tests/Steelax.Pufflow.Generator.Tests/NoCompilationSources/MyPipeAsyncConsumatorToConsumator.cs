using Steelax.Pufflow;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
internal partial class MyPipeAsyncConsumatorToConsumator<T1, T2>
{
    public MyPipeAsyncConsumatorToConsumator(IAsyncConsumator<T1> reader, out IConsumator<T2> output, FlowContext _)
    {
        _ = reader;
        output = null!;
    }
}