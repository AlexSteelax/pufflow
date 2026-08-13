using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Tests.Flows;

[Flow]
public partial class FlowSourceAsyncProducator<T>(IEnumerable<T> items)
{
    /// <remarks>
    ///     Толкает данные в target
    /// </remarks>
    public async Task ExecuteAsync(IAsyncProducator<T> target, FlowContext context)
    {
        try
        {
            foreach (var item in items)
            {
                context.Token.ThrowIfCancellationRequested();

                while (!target.TryWrite(item))
                {
                    context.Token.ThrowIfCancellationRequested();
                    await target.WaitToWriteAsync();
                }
            }
        }
        finally
        {
            target.Complete();
        }
    }

    /// <remarks>
    ///     Толкает данные в target
    /// </remarks>
    public async Task ExecuteAsync(IProducator<T> target, FlowContext context)
    {
        try
        {
            foreach (var item in items)
            {
                context.Token.ThrowIfCancellationRequested();

                while (!target.TryWrite(item))
                {
                    context.Token.ThrowIfCancellationRequested();
                    await Task.Delay(100, context.Token);
                }
            }
        }
        finally
        {
            target.Complete();
        }
    }
}