namespace Steelax.Pufflow.Operators;

/// <summary>Creates <see cref="IAsyncJob{TKey,TWarm}"/> instances for warming key segments.</summary>
/// <typeparam name="TKey">The key type used to partition the stream for warming.</typeparam>
/// <typeparam name="TWarm">The warming data type produced by an <see cref="IAsyncJob{TKey,TWarm}"/>.</typeparam>
[PublicAPI]
public interface IJobFactory<TKey, TWarm>
{
    /// <summary>Creates a new warming job.</summary>
    /// <returns>A fresh <see cref="IAsyncJob{TKey,TWarm}"/> instance.</returns>
    IAsyncJob<TKey, TWarm> CreateAsyncJob();
}
