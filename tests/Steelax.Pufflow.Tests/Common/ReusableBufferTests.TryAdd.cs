using Steelax.Pufflow.Common;

namespace Steelax.Pufflow.Tests.Common;

public static partial class ReusableBufferTests
{
    public sealed class TryAdd
    {
        [Fact]
        public void ShouldAddItem_WhenBufferIsNotFull()
        {
            var buffer = new ReusableBuffer<int>(3);

            var result = buffer.TryAdd(42);

            Assert.True(result);
            Assert.Equal(1, buffer.Count);
        }

        [Fact]
        public void ShouldReturnFalse_WhenBufferIsFull()
        {
            var buffer = new ReusableBuffer<int>(2);

            Assert.True(buffer.TryAdd(1));
            Assert.True(buffer.TryAdd(2));
            var result = buffer.TryAdd(3);

            Assert.False(result);
            Assert.Equal(2, buffer.Count);
        }
    }
}