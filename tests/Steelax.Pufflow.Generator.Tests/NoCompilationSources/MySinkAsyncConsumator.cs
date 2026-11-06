using Steelax.Pufflow;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
internal partial class MySinkAsyncConsumator<T>
{
    public MySinkAsyncConsumator(IAsyncConsumator<T> reader, FlowContext _)
    {
        _ = reader;
    }
}