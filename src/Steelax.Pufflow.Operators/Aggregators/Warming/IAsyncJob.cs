namespace Steelax.Pufflow.Operators.Aggregators.Warming;

/// <summary>
///     A two-phase warming job: <see cref="ExecuteAsync" /> performs the warming work asynchronously, writing
///     the warm data it produces into the supplied <see cref="ResultHolder{TKey,TWarm}" />; the framework applies
///     those results on the consumer loop after the task completes.
/// </summary>
/// <typeparam name="TKey">The key type used to partition the stream for warming.</typeparam>
/// <typeparam name="TWarm">The warming data type produced by the job.</typeparam>
/// <remarks>
///     <para>
///         The job is created by an <see cref="IJobFactory{TKey,TWarm}" /> and takes ownership of the keys
///         passed to <see cref="ExecuteAsync" /> for its lifetime. The warmer disposes it after the result has
///         been applied (or when the warmer itself is disposed).
///     </para>
///     <para>
    ///         The job writes a result for a key only when it produced warm data for it; keys it does not write are
    ///         reported downstream as warmed without data. All writes must happen before <see cref="ExecuteAsync" />
    ///         returns, so the returned task's completion publishes the results to the framework.
    ///     </para>
    ///     <para>
    ///         The implementation is responsible for providing the level of asynchrony that keeps the warmer's
    ///         consumer loop unblocked. The returned task should be genuinely asynchronous: heavy CPU-bound work
    ///         should be dispatched to a pool (for example via <c>Task.Run</c>), while pure I/O can rely on the
    ///         framework's async model without extra wrapping.
    ///     </para>
    /// </remarks>
[PublicAPI]
public interface IAsyncJob<TKey, TWarm> : IDisposable
    where TKey : notnull
{
    /// <summary>
    ///     Starts warming for the given keys, recording each produced result into <paramref name="holder" />.
    /// </summary>
    /// <param name="keys">The keys to warm; the job takes ownership of this array.</param>
    /// <param name="holder">
    ///     The sink for the warm results. Write a result per key that produced warm data; leave keys without
    ///     data unwritten. The holder is not thread-safe and must be written only from this job's execution
    ///     before the returned task completes.
    /// </param>
    /// <param name="cancellationToken">Cancels the warming work (e.g. when the warmer is disposed).</param>
    /// <returns>
    ///     A task that completes when the warming work is done. The returned task should complete without
    ///     holding the warmer's consumer loop: dispatch CPU-bound work to a pool (e.g. via <c>Task.Run</c>),
    ///     and avoid long synchronous sections in a single call.
    /// </returns>
    Task ExecuteAsync(TKey[] keys, ResultHolder<TKey, TWarm> holder, CancellationToken cancellationToken);
}