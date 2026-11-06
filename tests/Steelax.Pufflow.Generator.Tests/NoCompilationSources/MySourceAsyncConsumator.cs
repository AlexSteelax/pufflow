using Steelax.Pufflow;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
internal partial class MySourceAsyncConsumator<T>
{
    public MySourceAsyncConsumator(out IAsyncConsumator<T> output, FlowContext _)
    {
        output = null!;
    }
}