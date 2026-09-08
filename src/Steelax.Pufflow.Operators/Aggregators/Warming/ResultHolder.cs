using System.Runtime.InteropServices;

namespace Steelax.Pufflow.Operators.Aggregators.Warming;

/// <summary>
///     A write-only sink handed to an <see cref="IAsyncJob{TKey,TWarm}" /> so it can record the warm data
///     it produced for the segment's keys. The holder wraps a reusable <see cref="Dictionary{TKey,TWarm}" />
///     that the framework owns and recycles across segments; the job only fills it and never reads it back.
/// </summary>
/// <typeparam name="TKey">The key type used to partition the stream for warming.</typeparam>
/// <typeparam name="TWarm">The warming data type produced by the job.</typeparam>
/// <remarks>
///     <para>
///         A key with no warm data is simply <em>not written</em>: the framework treats the absence of a key
///         in the underlying buffer as "warmed without data". Writing the same key twice keeps the last value
///         (last-write-wins).
///     </para>
///     <para>
///         The holder is not thread-safe and must be used only from the job's warm thread, strictly before the
///         task returned by <see cref="IAsyncJob{TKey,TWarm}.ExecuteAsync" /> completes. The framework reads it
///         back after that completion, so the completion of the task acts as the publication barrier for all
///         writes made before it.
///     </para>
/// </remarks>
[PublicAPI]
public readonly struct ResultHolder<TKey, TWarm>
    where TKey : notnull
{
    private readonly Dictionary<TKey, TWarm> _buffer;
    
    internal ResultHolder(Dictionary<TKey, TWarm> buffer) => _buffer = buffer;

    /// <summary>
    ///     Records the warm data produced for <paramref name="key" />. Overwrites any earlier value for the
    ///     same key; a key that should be reported as warmed without data is simply not written.
    /// </summary>
    /// <param name="key">The warmed key.</param>
    /// <param name="warm">The warm data produced for the key.</param>
    [PublicAPI]
    public void Write(TKey key, TWarm warm) => CollectionsMarshal.GetValueRefOrAddDefault(_buffer, key, out _) = warm;
}