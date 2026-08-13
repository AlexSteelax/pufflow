 ﻿using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow;

/// <summary>
///     A marker struct that represents a data source emitting values of type <typeparamref name="T" /> via a poll
///     interface.
/// </summary>
/// <typeparam name="T">The poll output interface type (e.g., <see cref="IConsumator{T}" /> or <c>IEnumerator{T}</c>).</typeparam>
/// <remarks>
///     A Source component has no push input — it only exposes a poll (read) side.
///     Carries the component <see cref="Meta" /> and <see cref="Context" /> for the runtime pipeline builder.
/// </remarks>
[PublicAPI]
public readonly struct Source<T>
{
    /// <summary>
    ///     The node metadata that implements the source logic.
    /// </summary>
    internal readonly FlowMeta Meta;

    /// <summary>
    ///     The flow context providing cancellation support for the pipeline.
    /// </summary>
    internal readonly FlowContext Context;

    /// <summary>Initializes a source marker with its node metadata and flow context.</summary>
    internal Source(FlowMeta meta, FlowContext context)
    {
        Meta = meta;
        Context = context;
    }
}
