namespace Steelax.Pufflow.Operators.Aggregators.Warming;

/// <summary>
///     A two-phase warming job: <see cref="ExecuteAsync" /> runs on a background thread, while
///     <see cref="GetResult" /> and <see cref="SynchronousComplete" /> run on the consumer loop
///     after the task completes.
/// </summary>
/// <typeparam name="TKey">The key type used to partition the stream for warming.</typeparam>
/// <typeparam name="TWarm">The warming data type produced by the job.</typeparam>
/// <remarks>
///     The job is created by an <see cref="IJobFactory{TKey,TWarm}" /> and owns the keys passed to
///     <see cref="ExecuteAsync" /> for its lifetime. The warmer disposes it after the result has been
///     applied (or the warmer itself is disposed).
/// </remarks>
[PublicAPI]
public interface IAsyncJob<TKey, TWarm> : IDisposable
{
    /// <summary>Starts warming for the given keys on a background thread.</summary>
    /// <param name="keys">The keys to warm; the job takes ownership of this array.</param>
    /// <param name="cancellationToken">Cancels the warming work (e.g. when the warmer is disposed).</param>
    /// <returns>A task that completes when the warming work is done.</returns>
    Task ExecuteAsync(TKey[] keys, CancellationToken cancellationToken);

    /// <summary>Returns the warming data produced for the warmed keys, if any.</summary>
    /// <remarks>Must be called only after the task returned by <see cref="ExecuteAsync" /> has completed.</remarks>
    ReadOnlySpan<KeyValuePair<TKey, TWarm>> GetResult();

    /// <summary>Applies side effects on the consumer loop when the job completes.</summary>
    void SynchronousComplete();
}