namespace Steelax.Pufflow.Operators.Aggregators.Warming;

/// <summary>Creates <see cref="IAsyncJob{TKey,TWarm}" /> instances for warming key segments.</summary>
/// <typeparam name="TKey">The key type used to partition the stream for warming.</typeparam>
/// <typeparam name="TWarm">The warming data type produced by an <see cref="IAsyncJob{TKey,TWarm}" />.</typeparam>
/// <remarks>
///     Implementations are free to pool or allocate fresh jobs; each job receives a fresh
///     <see cref="ResultHolder{TKey,TWarm}" /> per segment.
/// </remarks>
[PublicAPI]
public interface IJobFactory<TKey, TWarm>
    where TKey : notnull
{
    /// <summary>Creates a new warming job.</summary>
    /// <returns>A fresh <see cref="IAsyncJob{TKey,TWarm}" /> instance.</returns>
    IAsyncJob<TKey, TWarm> CreateAsyncJob();
}