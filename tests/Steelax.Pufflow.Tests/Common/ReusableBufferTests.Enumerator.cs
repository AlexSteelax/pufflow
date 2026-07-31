using Steelax.Pufflow.Common;

namespace Steelax.Pufflow.Tests.Common;

public static partial class ReusableBufferTests
{
    public sealed class Enumerator
    {
        [Fact]
        public void ShouldAdvanceCorrectly()
        {
            var buffer = new ReusableBuffer<int>(3);
            buffer.TryAdd(1);
            buffer.TryAdd(2);

            using var enumerator = buffer.GetEnumerator();

            Assert.True(enumerator.MoveNext());
            Assert.Equal(1, enumerator.Current);

            Assert.True(enumerator.MoveNext());
            Assert.Equal(2, enumerator.Current);

            Assert.False(enumerator.MoveNext());
        }

        [Fact]
        public void GetEnumerator_ShouldReturnSameInstance()
        {
            var buffer = new ReusableBuffer<int>(3);
            buffer.TryAdd(42);

            using var enumerator1 = buffer.GetEnumerator();
            using var enumerator2 = buffer.GetEnumerator();

            Assert.Same(enumerator1, enumerator2);
        }

        [Fact]
        public void CurrentBeforeMoveNext_ShouldThrow()
        {
            var buffer = new ReusableBuffer<int>(3);
            using var enumerator = buffer.GetEnumerator();

            Assert.Throws<IndexOutOfRangeException>(() => enumerator.Current);
        }
    }
}