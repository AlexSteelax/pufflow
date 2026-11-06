using Steelax.Pufflow;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
internal partial class MySourceProducator<T>
{
    public MySourceProducator(IProducator<T> writer, FlowContext _)
    {
        _ = writer;
    }
}