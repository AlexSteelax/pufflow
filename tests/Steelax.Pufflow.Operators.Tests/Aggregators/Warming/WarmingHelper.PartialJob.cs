using JetBrains.Annotations;
using Steelax.Pufflow.Operators.Aggregators.Warming;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Warming;

internal static partial class WarmingHelper
{
    [PublicAPI]
    public sealed class PartialJobFactory(int[] warmedKeys) : IJobFactory<int, string>
    {
        public int CreatedCount { get; private set; }
        
        public IAsyncJob<int, string> CreateAsyncJob()
        {
            CreatedCount++;
            return new PartialJob(warmedKeys);
        }
    }
    
    /// <summary>A job that always faults with an exception.</summary>
    internal sealed class PartialJob(int[] warmedKeys) : IAsyncJob<int, string>
    {
        public Task ExecuteAsync(int[] keys, ResultHolder<int, string> holder, CancellationToken cancellationToken)
        {
            foreach (var key in keys.Intersect(warmedKeys))
                holder.Write(key, $"W{key}");

            return Task.CompletedTask;
        }

        public void Dispose() { }
    }
}