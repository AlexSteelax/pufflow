using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow;

/// <summary>
/// A marker struct that represents a data sink consuming values of type <typeparamref name="T"/> via a push interface.
/// </summary>
/// <typeparam name="T">The push input interface type (e.g., <see cref="IProducator{T}"/>).</typeparam>
/// <remarks>
/// A Sink component has no poll output — it only exposes a push (write) side and terminates the pipeline.
/// Carries the component <see cref="Instance"/> and <see cref="Context"/> for the runtime pipeline builder.
/// </remarks>
[PublicAPI]
public readonly struct Sink<T>(object instance, FlowContext context)
{
    /// <summary>
    /// The component instance that implements the sink logic.
    /// </summary>
    internal readonly object Instance = instance;

    /// <summary>
    /// The flow context providing cancellation support for the pipeline.
    /// </summary>
    internal readonly FlowContext Context = context;
}

/// <summary>
/// A marker struct that represents a data sink with an explicit sync/async mode tag.
/// </summary>
/// <typeparam name="TKind">A <see cref="Sync"/> or <see cref="Async"/> marker indicating the execution mode.</typeparam>
/// <typeparam name="T">The push input interface type (e.g., <see cref="IProducator{T}"/>).</typeparam>
/// <remarks>
/// The <typeparamref name="TKind"/> parameter enables compile-time disambiguation between
/// synchronous and asynchronous pipeline sinks.
/// </remarks>
[PublicAPI]
public readonly struct Sink<TKind, T>(object instance, FlowContext context)
{
    /// <summary>
    /// The component instance that implements the sink logic.
    /// </summary>
    internal readonly object Instance = instance;

    /// <summary>
    /// The flow context providing cancellation support for the pipeline.
    /// </summary>
    internal readonly FlowContext Context = context;
}
