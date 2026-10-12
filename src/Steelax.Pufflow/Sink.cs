 using Steelax.Pufflow.Abstractions;
 using Steelax.Pufflow.Runtime;

 namespace Steelax.Pufflow;

/// <summary>
///     A marker struct that represents a data sink consuming values of type <typeparamref name="T" /> via a push
///     interface.
/// </summary>
/// <typeparam name="T">The push input interface type (e.g., <see cref="IProducator{T}" />).</typeparam>
/// <remarks>
///     A Sink component has no poll output — it only exposes a push (write) side and terminates the pipeline.
///     Carries the component <see cref="Meta" /> and <see cref="FlowSource" /> for the runtime pipeline builder.
/// </remarks>
[PublicAPI]
public readonly struct Sink<T>
{
    /// <summary>
    ///     The node metadata that implements the sink logic.
    /// </summary>
    internal readonly FlowMeta Meta;

    /// <summary>
    ///     The flow source owning cancellation and background tasks for the pipeline.
    /// </summary>
    internal readonly FlowSource FlowSource;

    /// <summary>Initializes a sink marker with its node metadata and flow source.</summary>
    internal Sink(FlowMeta meta, FlowSource flowSource)
    {
        Meta = meta;
        FlowSource = flowSource;
    }
}
