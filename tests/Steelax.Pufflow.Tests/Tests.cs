using Steelax.Pufflow.Tests.Flows;

namespace Steelax.Pufflow.Tests;

public class Tests
{
    [Fact]
    public async Task Test1()
    {
        await using var source = new FlowSource(TestContext.Current.CancellationToken);

        var sourceFlow = new FlowSourceAsyncEnumerator<int>(Enumerable.Range(1, 5));
        var ret = new Queue<int>();
        
        await sourceFlow
            .Attach(source)
            .Next(new FlowPipeAsyncEnumeratorToAsyncEnumerator<int, int>(static v => v))
            .Next(new FlowPipeAsyncEnumeratorToAsyncEnumerator<int, int>(static v => v))
            .Next(new FlowSinkAsyncEnumerator<int>(ret))
            .ExecuteAsync();
        
        Assert.Equal(Enumerable.Range(1, 5), ret);
    }
    
    [Fact]
    public async Task Test2()
    {
        await using var source = new FlowSource(TestContext.Current.CancellationToken);

        var sourceFlow = new FlowSourceAsyncProducator<int>(Enumerable.Range(1, 5));
        var ret = new Queue<int>();

        _ = sourceFlow.AsyncFlowWithAsyncProducator.Attach(source);
        // .Next(new FlowPipeAsyncEnumeratorToAsyncEnumerator<int, int>(static v => v))
        // .Next(new FlowSinkAsyncEnumerator<int>(ret))
        // .ExecuteAsync();

        // Assert.Equal(Enumerable.Range(1, 5), ret);
    }
}