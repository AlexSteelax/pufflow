using System.ComponentModel;

namespace Steelax.Pufflow;

/// <summary>
///     A configured, source-side block factory produced by the source generator for a
///     <c>[Flow]</c> class. Construction of the concrete block instance is deferred until the composed
///     chain is built (at the terminal <c>End</c> or when the source is opened).
/// </summary>
/// <typeparam name="T">The flow interface this source produces (a pull reader or a push target).</typeparam>
[PublicAPI]
public readonly struct Source<T>
{
    private readonly Func<object> _lazy;

    /// <summary>Initializes a source factory from a lazy chain-producing delegate.</summary>
    /// <param name="lazy">A function producing the source's chain link.</param>
    /// <remarks>This constructor is part of the generated <c>[Flow]</c> contract and is not intended for direct use.</remarks>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public Source(Func<object> lazy) => _lazy = lazy;

    /// <summary>Materializes the source's chain link ({@link ConsumatorChain}/{@link ProducatorChain}).</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    internal object Invoke() => _lazy.Invoke();
}