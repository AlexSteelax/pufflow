using JetBrains.Annotations;
using Steelax.Pufflow.Operators.Aggregators.Warming;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Warming;

internal static partial class WarmingHelper
{
    [PublicAPI]
    public sealed class TcsJobFactory : IJobFactory<int, string>
    {
        public int CreatedCount { get; private set; }
        public List<TaskCompletionSource> Source { get; } = [];

        public IAsyncJob<int, string> CreateAsyncJob()
        {
            CreatedCount++;
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var job = new TcsJob(tcs);
            Source.Add(tcs);
            return job;
        }
    }
    
    /// <summary>A job whose completion is controlled by the test via a TCS (determinism).</summary>
    internal sealed class TcsJob(TaskCompletionSource tcs) : IAsyncJob<int, string>
    {
        public bool Started
        {
            get => Volatile.Read(ref field);
            private set => Volatile.Write(ref field, value);
        }
    
        public async Task ExecuteAsync(int[] keys, ResultHolder<int, string> holder, CancellationToken cancellationToken)
        {
            Started = true;
        
            await tcs.Task.WaitAsync(cancellationToken);
        
            foreach (var key in keys)
                holder.Write(key, $"W{key}");
        }

        public void Dispose() { }
    }
}