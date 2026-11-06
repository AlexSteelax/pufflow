using System.ComponentModel;

namespace Steelax.Pufflow;

/// <summary>
///     A configured sink block factory produced by the source generator for a <c>[Flow]</c> class.
///     Terminates a chain: its delegate consumes the upstream chain link and builds (or reaps) the final
///     block, typically registering background work or a consumer.
/// </summary>
/// <typeparam name="T">The flow interface this sink consumes (a pull reader or a handed-out push target).</typeparam>
[PublicAPI]
public readonly struct Sink<T>
{
    private readonly Action<FlowSource, object> _lazy;

    /// <summary>Initializes a sink factory from a terminating delegate.</summary>
    /// <param name="lazy">A delegate that builds the sink against the upstream chain link.</param>
    /// <remarks>This constructor is part of the generated <c>[Flow]</c> contract and is not intended for direct use.</remarks>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public Sink(Action<FlowSource, object> lazy) => _lazy = lazy;

    /// <summary>Terminates the chain, building the sink block against the given flow and upstream link.</summary>
    /// <param name="flowSource">The flow source owning the chain.</param>
    /// <param name="prev">The upstream chain link feeding this sink.</param>
    [EditorBrowsable(EditorBrowsableState.Never)]
    internal void Invoke(FlowSource flowSource, object prev) => _lazy.Invoke(flowSource, prev);
}