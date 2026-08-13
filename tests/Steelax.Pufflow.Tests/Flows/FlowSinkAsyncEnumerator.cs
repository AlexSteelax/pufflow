using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Tests.Flows;

[Flow]
public partial class FlowSinkAsyncEnumerator<T>(Queue<T> queue)
{
    /// <remarks>
    ///     Вытягивает данные из source
    /// </remarks>
    public async Task ExecuteAsync(IAsyncEnumerator<T> source, FlowContext context)
    {
        while (!context.Token.IsCancellationRequested && await source.MoveNextAsync())
            queue.Enqueue(source.Current);
    }
}