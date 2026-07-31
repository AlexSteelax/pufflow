using Steelax.Pufflow.Common;

namespace Steelax.Pufflow.Tests.Common;

public static partial class ReusableBufferTests
{
    public sealed class ValueType
    {
        [Fact]
        public void ShouldNotRetainOldValues_AfterReset()
        {
            var buffer = new ReusableBuffer<int>(3);
            buffer.TryAdd(10);
            buffer.TryAdd(20);

            buffer.Reset();
            buffer.TryAdd(30);

            // Single enumeration to verify (self-enumerator reuses instance)
            var items = buffer.ToArray();
            Assert.DoesNotContain(10, items);
            Assert.Single(items);
            Assert.Equal(30, items[0]);
        }
    }
}