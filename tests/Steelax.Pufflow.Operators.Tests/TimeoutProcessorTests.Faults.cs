namespace Steelax.Pufflow.Operators.Tests;

public static partial class TimeoutProcessorTests
{
    public sealed class Faults
    {
        [Fact]
        public async Task FaultedSource_Throws()
        {
            var ex = new InvalidOperationException("source error");
            var processor = new TimeoutProcessor<int>(TimeSpan.FromSeconds(5));

            await using var sourceEnumerator = FaultySourceAsync(ex).GetAsyncEnumerator(TestContext.Current.CancellationToken);
            await using var enumerator = processor.GetAsyncEnumerator(sourceEnumerator, default);

            var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                while (await enumerator.MoveNextAsync())
                {
                    // drain
                }
            });

            Assert.Same(ex, thrown);
        }
    }
}