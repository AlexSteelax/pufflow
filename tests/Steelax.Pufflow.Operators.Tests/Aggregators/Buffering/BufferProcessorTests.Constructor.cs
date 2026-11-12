using Steelax.Pufflow.Operators.Aggregators.Buffering;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Buffering;

public static partial class BufferProcessorTests
{
    public sealed class Constructor
    {
        [Fact]
        public void ValidCapacity_DoesNotThrow()
        {
            // Configuration must produce a usable pipe without throwing (a value type, so the call itself
            // is the assertion).
            _ = BypassBufferAProdToACons<int>.Configure(4);
        }

        [Fact]
        public void SyncSide_DoesNotThrow()
        {
            _ = BypassBufferProdToACons<int>.Configure(4);
        }
    }
}