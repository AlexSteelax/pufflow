using JetBrains.Annotations;
using Steelax.Pufflow.Operators.Aggregators.Warming;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Warming;

internal static partial class WarmingHelper
{
    [PublicAPI]
    public sealed class DelayedJobFactory(TimeSpan delay) : IJobFactory<int, string>
    {
        public int CreatedCount { get; private set; }

        public DelayedJobFactory(int millisecondsDelay) : this(TimeSpan.FromMilliseconds(millisecondsDelay)) { }
        
        public IAsyncJob<int, string> CreateAsyncJob()
        {
            CreatedCount++;
            return new DelayedJob(delay);
        }
    }
    
    /// <summary>A job that completes with a delay (warm = "W" + key).</summary>
    internal sealed class DelayedJob(TimeSpan delay) : IAsyncJob<int, string>
    {
        public async Task ExecuteAsync(int[] keys, ResultHolder<int, string> holder, CancellationToken cancellationToken)
        {
            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            
            foreach (var key in keys)
                holder.Write(key, $"W{key}");
        }

        public void Dispose() { }
    }
}