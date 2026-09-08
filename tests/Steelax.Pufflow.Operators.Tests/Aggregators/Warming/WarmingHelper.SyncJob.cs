using JetBrains.Annotations;
using Steelax.Pufflow.Operators.Aggregators.Warming;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Warming;

internal static partial class WarmingHelper
{
    [PublicAPI]
    public sealed class SyncJobFactory : IJobFactory<int, string>
    {
        public int CreatedCount { get; private set; }

        public IAsyncJob<int, string> CreateAsyncJob()
        {
            CreatedCount++;
            return new SyncJob();
        }
    }
    
    /// <summary>A job that completes synchronously (warm = "W" + key).</summary>
    internal sealed class SyncJob : IAsyncJob<int, string>
    {
        public Task ExecuteAsync(int[] keys, ResultHolder<int, string> holder, CancellationToken cancellationToken)
        {
            foreach (var key in keys)
                holder.Write(key, $"W{key}");

            return Task.CompletedTask;
        }

        public void Dispose() { }
    }
}