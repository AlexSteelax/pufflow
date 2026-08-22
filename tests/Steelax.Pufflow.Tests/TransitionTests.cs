using Steelax.Pufflow.Sdk.Test;
using Steelax.Pufflow.Tests.Flows;

namespace Steelax.Pufflow.Tests;

public class TransitionTests
{
    [Fact]
    public async Task AsyncConsumatorToAsyncProducatorChain_ProducesExpectedResults()
    {
        // Hybrid chain: a pull source (consumator) → a consumator→producator pipe (delayed binding:
        // the pipe needs the upstream consumator from the source and the target producator from the
        // sink) → a push sink (producator).
        await using var source = new FlowSource();

        var sourceFlow = source
            .OnAsyncConsumatorSource(Enumerable.Range(1, 5))
            .Next(new FlowPipeAsyncConsumatorToAsyncProducator<int, int>(static v => v * 10))
            .Next(new FlowPipeAsyncProducatorToAsyncProducator<int, int>(static v => v))
            .Consume(out var reader);

        await source.ExecuteAsync(TestContext.Current.CancellationToken);

        var ret = await reader.ReadAllAsync(TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal([10, 20, 30, 40, 50], ret);
    }
}