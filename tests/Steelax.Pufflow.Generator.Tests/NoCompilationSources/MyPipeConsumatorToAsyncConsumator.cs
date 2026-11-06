using Steelax.Pufflow;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
internal partial class MyPipeConsumatorToAsyncConsumator<T1, T2>
{
    public MyPipeConsumatorToAsyncConsumator(IConsumator<T1> reader, out IAsyncConsumator<T2> output, FlowContext _)
    {
        _ = reader;
        output = null!;
    }
}