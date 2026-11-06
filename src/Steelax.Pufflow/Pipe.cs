using System.ComponentModel;

namespace Steelax.Pufflow;

/// <summary>
///     A configured pipe block factory produced by the source generator for a <c>[Flow]</c> class.
///     Wraps a lazy link producer that, given the upstream chain link, returns the downstream chain link;
///     the concrete block instance is constructed when the chain is built.
/// </summary>
/// <typeparam name="TLeft">The flow interface this pipe consumes (a pull reader or an owned push producer).</typeparam>
/// <typeparam name="TRight">The flow interface this pipe produces (a pull reader or a push target).</typeparam>
[PublicAPI]
public readonly struct Pipe<TLeft, TRight>
{
    private readonly Func<object, object> _lazy;

    /// <summary>Initializes a pipe factory from a link-producing delegate.</summary>
    /// <param name="lazy">A function that turns the upstream chain link into this pipe's chain link.</param>
    /// <remarks>This constructor is part of the generated <c>[Flow]</c> contract and is not intended for direct use.</remarks>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public Pipe(Func<object, object> lazy) => _lazy = lazy;

    /// <summary>Connects this pipe to the upstream chain link and returns the downstream link.</summary>
    /// <param name="prev">The upstream chain link (an {@link ConsumatorChain} or {@link ProducatorChain}).</param>
    /// <returns>The downstream chain link, ready for further composition or terminal consumption.</returns>
    [EditorBrowsable(EditorBrowsableState.Never)]
    internal object Invoke(object prev) => _lazy.Invoke(prev);
}