using Steelax.Pufflow.Common;

namespace Steelax.Pufflow.Tests.Common;

public static partial class ReusableBufferTests
{
    public sealed class Reset
    {
        [Fact]
        public void ShouldClearBuffer_AndAllowReuse()
        {
            var buffer = new ReusableBuffer<string>(3);
            buffer.TryAdd("A");
            buffer.TryAdd("B");
            Assert.Equal(2, buffer.Count);

            buffer.Reset();
            Assert.Equal(0, buffer.Count);

            buffer.TryAdd("C");
            Assert.Equal(["C"], buffer.ToArray());
        }

        [Fact]
        public void EmptyBuffer_ShouldBeSafe()
        {
            var buffer = new ReusableBuffer<int>(3);

            buffer.Reset();

            Assert.Equal(0, buffer.Count);
            Assert.Empty(buffer.ToArray());
        }

        [Fact]
        public void MultipleCycles_ShouldWork()
        {
            var buffer = new ReusableBuffer<int>(3);

            for (var cycle = 0; cycle < 5; cycle++)
            {
                buffer.TryAdd(cycle * 10 + 1);
                buffer.TryAdd(cycle * 10 + 2);

                Assert.Equal([cycle * 10 + 1, cycle * 10 + 2], buffer.ToArray());

                buffer.Reset();
                Assert.Equal(0, buffer.Count);
            }
        }
    }
}