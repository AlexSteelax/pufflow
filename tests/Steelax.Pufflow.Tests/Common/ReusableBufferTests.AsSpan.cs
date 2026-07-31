using Steelax.Pufflow.Common;

namespace Steelax.Pufflow.Tests.Common;

public static partial class ReusableBufferTests
{
    public sealed class AsSpan
    {
        [Fact]
        public void ShouldReturnAllBufferedItems()
        {
            var buffer = new ReusableBuffer<int>(5);
            buffer.TryAdd(100);
            buffer.TryAdd(200);

            var span = buffer.AsSpan();

            Assert.Equal(2, span.Length);
            Assert.Equal(100, span[0]);
            Assert.Equal(200, span[1]);
        }

        [Fact]
        public void EmptyBuffer_ShouldReturnEmptySpan()
        {
            var buffer = new ReusableBuffer<int>(3);

            var span = buffer.AsSpan();

            Assert.True(span.IsEmpty);
        }
    }
}