using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Sdk.Test.PipeProcessors;

[Flow]
internal sealed partial class FlowPipeProducator<TSource, TTarget>(Func<TSource, TTarget> selector)
{
    private sealed class AsyncProducatorToAsyncConsumator(Func<TSource, TTarget> selector, CancellationToken cancellationToken) :
        IAsyncProducator<TSource>,
        IAsyncConsumator<TTarget>
    {
        private readonly Channel<TSource> _channel = Channel.CreateBounded<TSource>(2);
        
        public bool TryWrite(TSource value) => _channel.Writer.TryWrite(value);

        public bool TryComplete(Exception? ex = null) => _channel.Writer.TryComplete(ex);

        public ValueTask<bool> WaitToWriteAsync() => _channel.Writer.WaitToWriteAsync(cancellationToken);

        public bool TryRead([MaybeNullWhen(false)] out TTarget value)
        {
            if (_channel.Reader.TryRead(out var original))
            {
                value = selector.Invoke(original);
                return true;
            }
            
            value = default;
            return false;
        }

        public bool IsCompleted => _channel.Reader.Completion.IsCompleted;
        
        public ValueTask<bool> WaitToReadAsync() => _channel.Reader.WaitToReadAsync(cancellationToken);
    }
    
    private sealed class AsyncProducatorToAsyncProducator<TOutput>(TOutput target, Func<TSource, TTarget> selector, CancellationToken cancellationToken) :
        IAsyncProducator<TSource>
        where TOutput : IProducator<TTarget>
    {
        private readonly Channel<TSource> _channel = Channel.CreateBounded<TSource>(2);

        public async Task ExecuteAsync()
        {
            var asyncTarget = target as IAsyncProducator<TTarget>;

            try
            {
                await foreach (var item in _channel.Reader.ReadAllAsync())
                {
                    var value = selector.Invoke(item);

                    while (!target.TryWrite(value))
                    {
                        if (cancellationToken.IsCancellationRequested)
                            goto BreakLoop;

                        if (asyncTarget is not null)
                            await asyncTarget.WaitToWriteAsync();
                        else
                            await Task.Yield();
                    }
                }

                BreakLoop: ;
            }
            catch (Exception ex)
            {
                target.TryComplete(ex);
            }
            finally
            {
                target.TryComplete();
            }
        }

        public bool TryWrite(TSource value) => _channel.Writer.TryWrite(value);

        public bool TryComplete(Exception? ex = null) => _channel.Writer.TryComplete(ex);

        public ValueTask<bool> WaitToWriteAsync() => _channel.Writer.WaitToWriteAsync(cancellationToken);
    }
}