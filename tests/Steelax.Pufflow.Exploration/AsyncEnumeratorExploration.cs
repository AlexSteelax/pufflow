using Xunit;
// ReSharper disable AccessToDisposedClosure

namespace Steelax.Pufflow.Exploration;

public class ReflectionExploration
{
    private const string CustomException = "Custom Exception";
    
    [Fact(Timeout = 1000)]
    public async Task AsyncEnumerator_ImmediatelyThrow()
    {
        await using var enumerator = AsyncEnumerable<int>.GetImmediatelyThrow().GetAsyncEnumerator(TestContext.Current.CancellationToken);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () => await enumerator.MoveNextAsync());
        
        Assert.Equal(CustomException, ex.Message);
    }
    
    [Fact(Timeout = 1000)]
    public async Task AsyncEnumerator_OnceThenThrow()
    {
        await using var enumerator = AsyncEnumerable<int>.GetOnceThenThrow(5).GetAsyncEnumerator(TestContext.Current.CancellationToken);

        Assert.True(await enumerator.MoveNextAsync());
        Assert.Equal(5, enumerator.Current);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () => await enumerator.MoveNextAsync());
        
        Assert.Equal(CustomException, ex.Message);

        // Default value
        Assert.Equal(0, enumerator.Current);
    }

    private static class AsyncEnumerable<T>
    {
        public static async IAsyncEnumerable<T> GetImmediatelyThrow()
        {
            await Task.Yield();
            
            throw new InvalidOperationException(CustomException);
            
#pragma warning disable CS0162 // Unreachable code detected
            yield break;
#pragma warning restore CS0162 // Unreachable code detected
        }
        
        public static async IAsyncEnumerable<T> GetOnceThenThrow(T value)
        {
            await Task.Yield();
            
            yield return value;
            
            throw new InvalidOperationException(CustomException);
        }
    }
}