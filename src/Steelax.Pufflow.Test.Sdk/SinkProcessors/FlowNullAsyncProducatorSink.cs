using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Sdk.Test.SinkProcessors;

/// <summary>
///     An asynchronous push sink that hands out a discard target: every value written into it is dropped.
/// </summary>
/// <typeparam name="T">The element type.</typeparam>
[Flow]
internal sealed partial class FlowNullAsyncProducatorSink<T>
{
    /// <summary>Initializes the sink and hands out its discard target.</summary>
    /// <param name="writer">The asynchronous push target handed to the upstream chain.</param>
    /// <param name="_">The flow context (unused by this sink).</param>
    public FlowNullAsyncProducatorSink(out IAsyncProducator<T> writer, FlowContext _)
    {
        writer = new NullProducator();
    }

    private sealed class NullProducator : IAsyncProducator<T>
    {
        public bool TryWrite(T value) => true;

        public bool TryComplete(Exception? ex = null) => true;

        public bool IsFull => false;

        public ValueTask<bool> WaitToWriteAsync() => ValueTask.FromResult(true);
    }
}