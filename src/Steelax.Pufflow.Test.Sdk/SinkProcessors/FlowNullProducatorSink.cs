using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Sdk.Test.SinkProcessors;

/// <summary>
///     A synchronous push sink that hands out a discard target: every value written into it is dropped.
/// </summary>
/// <typeparam name="T">The element type.</typeparam>
[Flow]
internal sealed partial class FlowNullProducatorSink<T>
{
    /// <summary>Initializes the sink and hands out its discard target.</summary>
    /// <param name="writer">The synchronous push target handed to the upstream chain.</param>
    /// <param name="_">The flow context (unused by this sink).</param>
    public FlowNullProducatorSink(out IProducator<T> writer, FlowContext _)
    {
        writer = new NullProducator();
    }

    private sealed class NullProducator : IProducator<T>
    {
        public bool TryWrite(T value) => true;

        public bool TryComplete(Exception? ex = null) => true;

        public bool IsFull => false;
    }
}