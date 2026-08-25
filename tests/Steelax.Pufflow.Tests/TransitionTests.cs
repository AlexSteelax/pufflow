using Steelax.Pufflow.Sdk.Test;

namespace Steelax.Pufflow.Tests;

public class TransitionTests
{
    [Fact]
    public async Task AsyncConsumatorToAsyncProducatorChain_ProducesExpectedResults()
    {
        // Hybrid chain: a pull source (consumator) → a consumator→producator pipe → an async push pipe
        // → a push sink (producator).
        await using var source = new FlowSource();

        var sourceFlow = source
            .OnAsyncConsumatorSource(Enumerable.Range(1, 5))
            .ToAsyncProducator(static v => v * 10)
            .ToAsyncProducator(static v => v)
            .Consume(out var reader);

        await source.ExecuteAsync(TestContext.Current.CancellationToken);

        var ret = await reader.ReadAllAsync(TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal([10, 20, 30, 40, 50], ret);
    }
}
