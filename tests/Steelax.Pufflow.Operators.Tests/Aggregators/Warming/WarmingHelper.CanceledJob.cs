using JetBrains.Annotations;
using Steelax.Pufflow.Operators.Aggregators.Warming;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Warming;

internal static partial class WarmingHelper
{
    [PublicAPI]
    public sealed class CanceledJobFactory : IJobFactory<int, string>
    {
        public int CreatedCount { get; private set; }
        
        public IAsyncJob<int, string> CreateAsyncJob()
        {
            CreatedCount++;
            return new CanceledJob();
        }
    }
    
    /// <summary>A job that is always canceled.</summary>
    internal sealed class CanceledJob : IAsyncJob<int, string>
    {
        public Task ExecuteAsync(int[] keys, ResultHolder<int, string> holder, CancellationToken cancellationToken)
        {
            return Task.FromCanceled(new CancellationToken(true));
        }

        public void Dispose() { }
    }
}