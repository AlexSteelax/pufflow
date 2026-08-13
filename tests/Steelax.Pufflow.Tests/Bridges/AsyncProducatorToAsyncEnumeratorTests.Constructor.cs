using Steelax.Pufflow.Bridges;

namespace Steelax.Pufflow.Tests.Bridges;

public static partial class AsyncProducatorToAsyncEnumeratorTests
{
    public sealed class Constructor
    {
        [Theory]
        [InlineData(0)]
        [InlineData(-1)]
        public void NonPositiveLimit_Throws(int limit)
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new AsyncProducatorToAsyncEnumerator<int>(limit));
        }
    }
}