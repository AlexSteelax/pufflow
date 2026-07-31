using Steelax.Pufflow.Common;

namespace Steelax.Pufflow.Tests.Common;

public static partial class ReusableBufferTests
{
    public sealed class Count
    {
        [Fact]
        public void ShouldReflectAddedItems()
        {
            var buffer = new ReusableBuffer<int>(10);

            Assert.Equal(0, buffer.Count);
            buffer.TryAdd(1);
            Assert.Equal(1, buffer.Count);
            buffer.TryAdd(2);
            Assert.Equal(2, buffer.Count);
        }

        [Fact]
        public void ShouldBeZero_AfterReset()
        {
            var buffer = new ReusableBuffer<int>(5);
            buffer.TryAdd(1);
            buffer.TryAdd(2);
            Assert.Equal(2, buffer.Count);

            buffer.Reset();

            Assert.Equal(0, buffer.Count);
        }
    }
}