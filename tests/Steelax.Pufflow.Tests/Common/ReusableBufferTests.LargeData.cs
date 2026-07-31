using Steelax.Pufflow.Common;

namespace Steelax.Pufflow.Tests.Common;

public static partial class ReusableBufferTests
{
    public sealed class LargeData
    {
        [Fact]
        public void LargeNumberOfItems_ShouldWork()
        {
            const int capacity = 1000;
            var buffer = new ReusableBuffer<int>(capacity);

            for (var i = 0; i < capacity; i++)
                Assert.True(buffer.TryAdd(i));

            Assert.Equal(capacity, buffer.Count);
            Assert.Equal(Enumerable.Range(0, capacity), buffer.ToArray());
        }
    }
}