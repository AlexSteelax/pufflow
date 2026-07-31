using Steelax.Pufflow.Common;

namespace Steelax.Pufflow.Tests.Common;

public static partial class ReusableBufferTests
{
    public sealed class Dispose
    {
        [Fact]
        public void MultipleCalls_ShouldBeSafe()
        {
            var buffer = new ReusableBuffer<int>(3);

            buffer.Dispose();
            buffer.Dispose();

            Assert.Equal(0, buffer.Count);
        }
    }
}