 ﻿using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow;

/// <summary>
///     A marker struct that represents a data sink consuming values of type <typeparamref name="T" /> via a push
///     interface.
/// </summary>
/// <typeparam name="T">The push input interface type (e.g., <see cref="IProducator{T}" />).</typeparam>
/// <remarks>
///     A Sink component has no poll output — it only exposes a push (write) side and terminates the pipeline.
///     Carries the component <see cref="Meta" /> and <see cref="Context" /> for the runtime pipeline builder.
/// </remarks>
[PublicAPI]
public readonly struct Sink<T>
{
    /// <summary>
    ///     The node metadata that implements the sink logic.
    /// </summary>
    internal readonly FlowMeta Meta;

    /// <summary>
    ///     The flow context providing cancellation support for the pipeline.
    /// </summary>
    internal readonly FlowContext Context;

    /// <summary>Initializes a sink marker with its node metadata and flow context.</summary>
    internal Sink(FlowMeta meta, FlowContext context)
    {
        Meta = meta;
        Context = context;
    }
}
