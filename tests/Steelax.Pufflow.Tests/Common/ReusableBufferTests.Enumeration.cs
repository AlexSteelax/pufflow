using Steelax.Pufflow.Common;

namespace Steelax.Pufflow.Tests.Common;

public static partial class ReusableBufferTests
{
    public sealed class Enumeration
    {
        [Fact]
        public void ToArray_ShouldReturnAllAddedItems()
        {
            var buffer = new ReusableBuffer<int>(5);
            buffer.TryAdd(10);
            buffer.TryAdd(20);
            buffer.TryAdd(30);

            var result = buffer.ToArray();

            Assert.Equal([10, 20, 30], result);
        }

        [Fact]
        public void Foreach_ShouldIterateOverAllItems()
        {
            var buffer = new ReusableBuffer<int>(4);
            buffer.TryAdd(1);
            buffer.TryAdd(2);
            buffer.TryAdd(3);

            var items = new List<int>();
            foreach (var item in buffer)
                items.Add(item);

            Assert.Equal([1, 2, 3], items);
        }

        [Fact]
        public void EmptyBuffer_ShouldReturnEmpty()
        {
            var buffer = new ReusableBuffer<int>(3);

            var result = buffer.ToArray();

            Assert.Empty(result);
        }
    }
}