using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow;

/// <summary>
/// A marker struct that represents a data source emitting values of type <typeparamref name="T"/> via a poll interface.
/// </summary>
/// <typeparam name="T">The poll output interface type (e.g., <see cref="IConsumator{T}"/> or <c>IEnumerator{T}</c>).</typeparam>
/// <remarks>
/// A Source component has no push input — it only exposes a poll (read) side.
/// Carries the component <see cref="Instance"/> and <see cref="Context"/> for the runtime pipeline builder.
/// </remarks>
[PublicAPI]
public readonly struct Source<T>(object instance, FlowContext context)
{
    /// <summary>
    /// The component instance that implements the source logic.
    /// </summary>
    internal readonly object Instance = instance;

    /// <summary>
    /// The flow context providing cancellation support for the pipeline.
    /// </summary>
    internal readonly FlowContext Context = context;
}

/// <summary>
/// A marker struct that represents a data source with an explicit sync/async mode tag.
/// </summary>
/// <typeparam name="TVoid">A <see cref="Sync"/> or <see cref="Async"/> marker indicating the execution mode.</typeparam>
/// <typeparam name="T">The poll output interface type (e.g., <see cref="IConsumator{T}"/> or <c>IEnumerator{T}</c>).</typeparam>
/// <remarks>
/// The <typeparamref name="TVoid"/> parameter enables compile-time disambiguation between
/// synchronous and asynchronous pipeline sources.
/// </remarks>
[PublicAPI]
public readonly struct Source<TVoid, T>(object instance, FlowContext context)
{
    /// <summary>
    /// The component instance that implements the source logic.
    /// </summary>
    internal readonly object Instance = instance;

    /// <summary>
    /// The flow context providing cancellation support for the pipeline.
    /// </summary>
    internal readonly FlowContext Context = context;
}
