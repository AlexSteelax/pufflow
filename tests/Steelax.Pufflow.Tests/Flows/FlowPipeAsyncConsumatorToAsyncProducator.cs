using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Tests.Flows;

[Flow]
public partial class FlowPipeAsyncConsumatorToAsyncProducator<T1, T2>
{
    private readonly Func<T1, T2> _transform;

    public FlowPipeAsyncConsumatorToAsyncProducator(Func<T1, T2> transform)
    {
        _transform = transform;
    }

    public void Fuse(IAsyncConsumator<T1> source, IAsyncProducator<T2> target, FlowContext context)
    {
        context.RegisterBackground(() => PumpAsync(source, target, _transform, context));
    }

    private static async Task PumpAsync(IAsyncConsumator<T1> source, IAsyncProducator<T2> target,
        Func<T1, T2> transform, FlowContext context)
    {
        try
        {
            while (!context.Token.IsCancellationRequested)
            {
                if (source.TryRead(out var item))
                {
                    while (!target.TryWrite(transform.Invoke(item)))
                        await target.WaitToWriteAsync();
                    continue;
                }

                if (source.IsCompleted)
                    break;

                await source.WaitToReadAsync();
            }
        }
        finally
        {
            target.TryComplete();
        }
    }
}
