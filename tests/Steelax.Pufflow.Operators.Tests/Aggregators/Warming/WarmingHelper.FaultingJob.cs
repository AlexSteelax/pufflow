using JetBrains.Annotations;
using Steelax.Pufflow.Operators.Aggregators.Warming;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Warming;

internal static partial class WarmingHelper
{
    [PublicAPI]
    public sealed class FaultingJobFactory : IJobFactory<int, string>
    {
        public int CreatedCount { get; private set; }
        
        public static readonly InvalidOperationException Boom = new("boom");
        
        public IAsyncJob<int, string> CreateAsyncJob()
        {
            CreatedCount++;
            return new FaultingJob(Boom);
        }
    }
    
    /// <summary>A job that always faults with an exception.</summary>
    internal sealed class FaultingJob(Exception ex) : IAsyncJob<int, string>
    {
        public Task ExecuteAsync(int[] keys, ResultHolder<int, string> holder, CancellationToken cancellationToken)
        {
            return Task.FromException(ex);
        }

        public void Dispose() { }
    }
}