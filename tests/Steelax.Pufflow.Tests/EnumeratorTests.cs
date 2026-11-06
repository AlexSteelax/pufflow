using Steelax.Pufflow.Sdk.Test;

namespace Steelax.Pufflow.Tests;

public class EnumeratorTests
{
    [Fact(Timeout = 2000)]
    public async Task EnumeratorChain_ProducesExpectedResults()
    {
        await using var flow = new FlowSource();

        flow
            .OnEnumeratorSource(out var writer, Enumerable.Range(1, 5))
            .ToAsyncEnumerator(static v => v * 10)
            .Consume(out var reader);

        writer.TryComplete();

        await flow.ExecuteAsync(TestContext.Current.CancellationToken);

        var result = await reader.ReadAllAsync(TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(Enumerable.Range(1, 5).Select(static v => v * 10), result);
    }

    [Fact(Timeout = 2000)]
    public async Task EnumeratorToAsyncConsumator_ProducesExpectedResults()
    {
        await using var flow = new FlowSource();

        flow
            .OnEnumeratorSource(out var writer, Enumerable.Range(1, 5))
            .ToAsyncConsumator(static v => v * 2)
            .Consume(out var reader);

        writer.TryComplete();

        await flow.ExecuteAsync(TestContext.Current.CancellationToken);

        var result = await reader.ReadAllAsync(TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(Enumerable.Range(1, 5).Select(static v => v * 2), result);
    }

    [Fact(Timeout = 2000)]
    public async Task ConsumatorToAsyncEnumerator_ProducesExpectedResults()
    {
        await using var flow = new FlowSource();

        flow
            .OnAsyncConsumatorSource(Enumerable.Range(1, 5))
            .ToAsyncEnumerator(static v => v * 3)
            .Consume(out var reader);

        await flow.ExecuteAsync(TestContext.Current.CancellationToken);

        var result = await reader.ReadAllAsync(TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(Enumerable.Range(1, 5).Select(static v => v * 3), result);
    }
}