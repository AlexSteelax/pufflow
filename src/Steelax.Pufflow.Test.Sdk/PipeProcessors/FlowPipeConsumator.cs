using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Sdk.Test.PipeProcessors;

[Flow]
internal sealed partial class FlowPipeConsumator<TSource, TTarget>(Func<TSource, TTarget> selector)
{
    private sealed class AsyncConsumatorToAsyncConsumator<TInput>(TInput source, Func<TSource, TTarget> selector, CancellationToken cancellationToken) : IAsyncConsumator<TTarget>
        where TInput : IConsumator<TSource>
    {
        private readonly Channel<TSource> _channel = Channel.CreateBounded<TSource>(2);
        
        public async Task ExecuteAsync()
        {
            var asyncSource = source as IAsyncConsumator<TSource>;
            
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    if (!source.TryRead(out var value))
                    {
                        if (source.IsCompleted)
                            goto BreakLoop;
                        
                        if (asyncSource is not null)
                        {
                            if (!await asyncSource.WaitToReadAsync())
                                goto BreakLoop;
                        }
                        else
                        {
                            await Task.Yield();
                        }

                        continue;
                    }
                    
                    await _channel.Writer.WriteAsync(value);
                }
                
                BreakLoop: ;
            }
            catch (Exception ex)
            {
                _channel.Writer.TryComplete(ex);
            }
            finally
            {
                _channel.Writer.TryComplete();
            }
        }
        
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
    
    private sealed class AsyncConsumatorToAsyncProducator<TInput, TOutput>(TInput input, TOutput output, Func<TSource, TTarget> selector, CancellationToken cancellationToken)
        where TInput : IConsumator<TSource>
        where TOutput : IProducator<TTarget>
    {
        public async Task ExecuteAsync()
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var read = await TryReadAsync(input, cancellationToken);
                    if (!read.HasValue)
                        break;

                    var value = selector.Invoke(read.Value);
                    await WriteAsync(output, value, cancellationToken);
                }
            }
            catch (Exception ex)
            {
                output.TryComplete(ex);
            }
            finally
            {
                output.TryComplete();
            }
        }

        private static async ValueTask WriteAsync(TOutput output, TTarget value, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                if (output.TryWrite(value))
                    return;

                if (output is IAsyncProducator<TOutput> asyncOutput)
                {
                    await asyncOutput.WaitToWriteAsync();
                }
                else
                {
                    await Task.Yield();
                }
            }
        }

        private static async ValueTask<ReadResult> TryReadAsync(TInput input, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                if (input.TryRead(out var value))
                    return new ReadResult(true, value);

                if (input is IAsyncConsumator<TSource> asyncInput)
                {
                    if (!await asyncInput.WaitToReadAsync())
                        break;
                }
                else
                {
                    await Task.Yield();
                    if (input.IsCompleted)
                        break;
                }
            }

            return default;
        }

        private readonly record struct ReadResult(bool HasValue, TSource Value);
    }
}