using Steelax.Pufflow;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
internal partial class MySourceAsyncProducator<T>
{
    public MySourceAsyncProducator(IAsyncProducator<T> writer, FlowContext _)
    {
        _ = writer;
    }
}