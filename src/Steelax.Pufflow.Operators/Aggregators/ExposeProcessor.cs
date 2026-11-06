namespace Steelax.Pufflow.Operators.Aggregators;

/// <summary>
///     A terminal probe that captures the upstream pull interface and hands it to an external callback.
///     Used by <c>Expose</c> to expose the readable side of a pipeline out to the caller without consuming it.
/// </summary>
/// <typeparam name="T">The element type flowing through the stage.</typeparam>
[Flow]
internal sealed partial class ExposeProcessor<T>
{
    /// <summary>Captures the upstream source — the stage is passive and only relays the interface.</summary>
    /// <param name="assign">Receives the upstream pull interface so the caller can read from it.</param>
    /// <param name="source">The upstream asynchronous pull stream, captured into the callback.</param>
    /// <param name="_">The flow context (unused by this stage).</param>
    public ExposeProcessor(Action<IAsyncConsumator<T>> assign, IAsyncConsumator<T> source, FlowContext _)
    {
        assign(source);
    }
}