using Steelax.Pufflow;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

[Flow]
internal partial class MyConstrained<T, TBatch>
    where T : notnull
{
    public MyConstrained(IAsyncConsumator<T> reader, out IAsyncConsumator<TBatch> output, FlowContext _)
    {
        _ = reader;
        output = null!;
    }
}