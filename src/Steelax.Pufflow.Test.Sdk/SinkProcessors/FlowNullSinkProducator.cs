using JetBrains.Annotations;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Sdk.Test.SinkProcessors;

[Flow]
internal partial class FlowNullSinkProducator<T>
{
    [PublicAPI]
    public void Fuse(out IAsyncProducator<T> target, FlowContext context)
    {
        target = new AsyncProducator();
    }

    [PublicAPI]
    public void Fuse(out IProducator<T> target, FlowContext context)
    {
        target = new AsyncProducator();
    }

    private sealed class AsyncProducator : IAsyncProducator<T>
    {
        public bool TryWrite(T _) => true;

        public bool TryComplete(Exception? ex = null) => true;

        public ValueTask<bool> WaitToWriteAsync() => ValueTask.FromResult(true);
    }
}
