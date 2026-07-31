using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests.NoCompilationSources;

public class BatchBase<T> { }

public interface IBatchFactory<TBatch, T> where TBatch : BatchBase<T>
{
    static abstract TBatch Create(int size);
}

[Flow]
public partial class MyConstrained<T, TBatch> where TBatch : BatchBase<T>, IBatchFactory<TBatch, T>
{
    public System.Collections.Generic.IAsyncEnumerator<TBatch> GetAsyncEnumerator(
        System.Collections.Generic.IAsyncEnumerator<T> source,
        Steelax.Pufflow.FlowContext ctx)
    {
        throw new System.NotImplementedException();
    }
}
