using System.Collections.Generic;
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
    public void Fuse(in IAsyncEnumerator<T> source, out IAsyncEnumerator<TBatch> target, Steelax.Pufflow.FlowContext ctx)
    {
        target = null!;
    }
}
