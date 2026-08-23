using Steelax.Pufflow.Sdk.Test;
using Steelax.Pufflow.Tests.Flows;

namespace Steelax.Pufflow.Tests;

public class BaseTests
{
    [Fact]
    public async Task AsyncEnumeratorChain_ProducesExpectedResults()
    {
        await using var source = new FlowSource();

        var sourceFlow = source
            .OnAsyncEnumeratorSource(Enumerable.Range(1, 5))
            .Next(new FlowPipeAsyncEnumeratorToAsyncEnumerator<int, int>(static v => v))
            .Next(new FlowPipeAsyncEnumeratorToAsyncEnumerator<int, int>(static v => v))
            .Consume(out var reader);

        await source.ExecuteAsync(TestContext.Current.CancellationToken);

        var ret = await reader.ReadAllAsync(TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(Enumerable.Range(1, 5), ret);
    }

    [Fact]
    public async Task AsyncConsumatorChain_ProducesExpectedResults()
    {
        await using var source = new FlowSource();

        var sourceFlow = source
            .OnAsyncConsumatorSource(Enumerable.Range(1, 5))
            .Next(new FlowPipeAsyncConsumatorToAsyncConsumator<int, int>(static v => v))
            .Next(new FlowPipeAsyncConsumatorToAsyncConsumator<int, int>(static v => v))
            .Consume(out var reader);

        await source.ExecuteAsync(TestContext.Current.CancellationToken);

        var ret = await reader.ReadAllAsync(TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(Enumerable.Range(1, 5), ret);
    }

    [Fact]
    public async Task AsyncProducatorChain_ProducesExpectedResults()
    {
        await using var source = new FlowSource();

        var sourceFlow = source
            .OnAsyncProducatorSource(Enumerable.Range(1, 5))
            .Next(new FlowPipeAsyncProducatorToAsyncProducator<int, int>(static v => v))
            .Next(new FlowPipeAsyncProducatorToAsyncProducator<int, int>(static v => v))
            .Consume(out var reader);

        await source.ExecuteAsync(TestContext.Current.CancellationToken);

        var ret = await reader.ReadAllAsync(TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(Enumerable.Range(1, 5), ret);
    }

    [Fact]
    public async Task ProducatorChain_ProducesExpectedResults()
    {
        await using var source = new FlowSource();

        source
            .OnProducatorSource(Enumerable.Range(1, 5))
            .Next(new FlowPipeProducatorToProducator<int, int>(static v => v))
            .Next(new FlowPipeProducatorToProducator<int, int>(static v => v))
            .Consume(out var reader);

        await source.ExecuteAsync(TestContext.Current.CancellationToken);

        var ret = await reader.ReadAllAsync(TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(Enumerable.Range(1, 5), ret);
    }
}
