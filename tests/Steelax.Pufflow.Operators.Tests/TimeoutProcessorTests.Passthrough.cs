namespace Steelax.Pufflow.Operators.Tests;

public static partial class TimeoutProcessorTests
{
    public sealed class Passthrough
    {
        [Fact]
        public async Task FastSource_YieldsAllItemsInOrder_WithoutMarkers()
        {
            var processor = new TimeoutProcessor<int>(TimeSpan.FromSeconds(5));
            var result = await CollectAsync(processor, new[] { 1, 2, 3 }.ToAsyncEnumerable());

            Assert.All(result, value => Assert.True(value.IsT0));
            Assert.Equal(new[] { 1, 2, 3 }, result.Select(value => value.AsT0));
        }

        [Fact]
        public async Task EmptySource_YieldsNothing()
        {
            var processor = new TimeoutProcessor<int>(TimeSpan.FromSeconds(5));
            var result = await CollectAsync(processor, Array.Empty<int>().ToAsyncEnumerable());

            Assert.Empty(result);
        }
    }
}