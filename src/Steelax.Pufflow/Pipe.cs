using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow;

/// <summary>
///     A marker struct that represents a data transformer pipe from <typeparamref name="TLeft" /> to
///     <typeparamref name="TRight" />.
/// </summary>
/// <typeparam name="TLeft">The push input type (e.g., <see cref="IProducator{T}" />).</typeparam>
/// <typeparam name="TRight">The poll output type (e.g., <see cref="IConsumator{T}" />).</typeparam>
/// <remarks>
///     Encodes a two-interface shape: push input → poll output.
///     Carries the component <see cref="Instance" /> and <see cref="Context" /> for the runtime pipeline builder.
/// </remarks>
[PublicAPI]
public readonly struct Pipe<TLeft, TRight>(object instance, FlowContext context)
{
    /// <summary>
    ///     The component instance that implements the pipe logic.
    /// </summary>
    internal readonly object Instance = instance;

    /// <summary>
    ///     The flow context providing cancellation support for the pipeline.
    /// </summary>
    internal readonly FlowContext Context = context;
}

/// <summary>
///     A marker struct that represents a data transformer pipe with an explicit sync/async mode tag.
/// </summary>
/// <typeparam name="TKind">A <see cref="Sync" /> or <see cref="Async" /> marker indicating the execution mode.</typeparam>
/// <typeparam name="TLeft">The push input type (e.g., <see cref="IProducator{T}" />).</typeparam>
/// <typeparam name="TRight">The poll output type (e.g., <see cref="IConsumator{T}" />).</typeparam>
/// <remarks>
///     The <typeparamref name="TKind" /> parameter enables compile-time disambiguation between
///     synchronous and asynchronous pipeline stages.
/// </remarks>
[PublicAPI]
public readonly struct Pipe<TKind, TLeft, TRight>(object instance, FlowContext context)
{
    /// <summary>
    ///     The component instance that implements the pipe logic.
    /// </summary>
    internal readonly object Instance = instance;

    /// <summary>
    ///     The flow context providing cancellation support for the pipeline.
    /// </summary>
    internal readonly FlowContext Context = context;
}