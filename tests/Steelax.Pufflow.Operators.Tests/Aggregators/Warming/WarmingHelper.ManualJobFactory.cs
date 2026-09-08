using JetBrains.Annotations;
using Steelax.Pufflow.Operators.Aggregators.Warming;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Warming;

internal static partial class WarmingHelper
{
    [PublicAPI]
    public sealed class ManualJobFactory(Func<IAsyncJob<int, string>> job) : IJobFactory<int, string>
    {
        public IAsyncJob<int, string> CreateAsyncJob()
        {
            return job.Invoke();
        }
    }
}