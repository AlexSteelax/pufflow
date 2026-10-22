namespace Steelax.Pufflow.Operators.Aggregators;

/// <summary>
///     A terminal probe that captures the upstream pull interface and exposes it externally. Used by
///     <c>Expose</c> to hand the readable side of a pipeline out to the caller without consuming it.
/// </summary>
/// <typeparam name="T">The element type flowing through the stage.</typeparam>
[Flow]
internal partial class ExposeProcessor<T>
{
    /// <summary>The upstream pull interface, captured during fusion.</summary>
    public IAsyncConsumator<T> Source { get; private set; } = null!;
    
    /// <summary>Captures the upstream source — the stage is passive and only relays the interface.</summary>
    public void Fuse(IAsyncConsumator<T> source, FlowContext context)
    {
        Source = source;
    }
}