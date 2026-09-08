namespace Steelax.Pufflow.Operators.Tests.Aggregators.Warming;

internal static partial class WarmingHelper
{
    public sealed class RecordingCallback
    {
        private int _count;
        
        public int Count => Volatile.Read(ref _count);

        public void Invoke() => Interlocked.Increment(ref _count);
    }
}