using Steelax.Pufflow.Common;

namespace Steelax.Pufflow.Tests.Common;

public static partial class ReusableBufferTests
{
    public sealed class ReferenceType
    {
        [Fact]
        public void ResetShouldClearStaleReferences()
        {
            var buffer = new ReusableBuffer<string>(3);
            buffer.TryAdd("hello");
            buffer.TryAdd("world");

            buffer.Reset();

            buffer.TryAdd("new");
            Assert.Equal(["new"], buffer.ToArray());
        }
    }
}