 ﻿using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow;

/// <summary>
///     A marker struct that represents a data transformer pipe from <typeparamref name="TLeft" /> to
///     <typeparamref name="TRight" />.
/// </summary>
/// <typeparam name="TLeft">The push input type (e.g., <see cref="IProducator{T}" />).</typeparam>
/// <typeparam name="TRight">The poll output type (e.g., <see cref="IConsumator{T}" />).</typeparam>
/// <remarks>
///     Encodes a two-interface shape: push input → poll output.
///     Carries the component <see cref="Meta" /> and <see cref="Context" /> for the runtime pipeline builder.
/// </remarks>
[PublicAPI]
public readonly struct Pipe<TLeft, TRight>
{
    /// <summary>
    ///     The node metadata that implements the pipe logic.
    /// </summary>
    internal readonly FlowMeta Meta;

    /// <summary>
    ///     The flow context providing cancellation support for the pipeline.
    /// </summary>
    internal readonly FlowContext Context;

    /// <summary>Initializes a pipe marker with its node metadata and flow context.</summary>
    internal Pipe(FlowMeta meta, FlowContext context)
    {
        Meta = meta;
        Context = context;
    }
}
