using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Tests.Flows;

[Flow]
public partial class FlowPipeAsyncProducatorToAsyncProducator<T1, T2>(Func<T1, T2> transform)
{
    public void Fuse(out IAsyncProducator<T1> source, IAsyncProducator<T2> target, FlowContext context)
    {
        source = new AsyncProducator(target, transform);
    }

    private sealed class AsyncProducator(IAsyncProducator<T2> target, Func<T1, T2> transform) : IAsyncProducator<T1>
    {
        public bool TryWrite(T1 value) => target.TryWrite(transform.Invoke(value));

        public bool TryComplete(Exception? ex = null) => target.TryComplete(ex);

        public ValueTask<bool> WaitToWriteAsync() => target.WaitToWriteAsync();
    }
}