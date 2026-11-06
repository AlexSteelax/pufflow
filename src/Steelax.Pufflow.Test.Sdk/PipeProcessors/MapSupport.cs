using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Sdk.Test.PipeProcessors;

/// <summary>
///     Shared adapters and pump loops backing the map pipe processors. Kept internal so the generated
///     <c>Configure</c> factories stay tiny.
/// </summary>
internal static class MapSupport
{
    #region Readers / writers over channels

    internal sealed class SyncReader<T>(ChannelReader<T> reader) : IConsumator<T>
    {
        public bool TryRead([MaybeNullWhen(false)] out T value) => reader.TryRead(out value);

        public bool IsCompleted => reader.Completion.IsCompleted;
    }

    internal sealed class AsyncReader<T>(ChannelReader<T> reader) : IAsyncConsumator<T>
    {
        public bool TryRead([MaybeNullWhen(false)] out T value) => reader.TryRead(out value);

        public bool IsCompleted => reader.Completion.IsCompleted;

        public ValueTask<bool> WaitToReadAsync() => reader.WaitToReadAsync();
    }

    internal sealed class SyncWriter<T>(ChannelWriter<T> writer) : IProducator<T>
    {
        public bool TryWrite(T value) => writer.TryWrite(value);

        public bool TryComplete(Exception? ex = null) => writer.TryComplete(ex);

        public bool IsFull => false;
    }

    internal sealed class AsyncWriter<T>(ChannelWriter<T> writer) : IAsyncProducator<T>
    {
        public bool TryWrite(T value) => writer.TryWrite(value);

        public bool TryComplete(Exception? ex = null) => writer.TryComplete(ex);

        public bool IsFull => false;

        public ValueTask<bool> WaitToWriteAsync() => writer.WaitToWriteAsync();
    }

    #endregion

    #region Mapping readers (composite push -> pull)

    internal sealed class MapReader<T1, T2>(ChannelReader<T1> reader, Func<T1, T2> selector) : IConsumator<T2>
    {
        public bool TryRead([MaybeNullWhen(false)] out T2 value)
        {
            if (reader.TryRead(out var original))
            {
                value = selector(original);
                return true;
            }

            value = default;
            return false;
        }

        public bool IsCompleted => reader.Completion.IsCompleted;
    }

    internal sealed class MapAsyncReader<T1, T2>(ChannelReader<T1> reader, Func<T1, T2> selector) : IAsyncConsumator<T2>
    {
        public bool TryRead([MaybeNullWhen(false)] out T2 value)
        {
            if (reader.TryRead(out var original))
            {
                value = selector(original);
                return true;
            }

            value = default;
            return false;
        }

        public bool IsCompleted => reader.Completion.IsCompleted;

        public ValueTask<bool> WaitToReadAsync() => reader.WaitToReadAsync();
    }

    #endregion

    #region Mapping producers (push -> push)

    internal sealed class MapProducator<T1, T2>(IProducator<T2> target, Func<T1, T2> selector) : IProducator<T1>
    {
        public bool TryWrite(T1 value) => target.TryWrite(selector(value));

        public bool TryComplete(Exception? ex = null) => target.TryComplete(ex);

        public bool IsFull => target.IsFull;
    }

    internal sealed class MapAsyncProducator<T1, T2>(IProducator<T2> target, Func<T1, T2> selector) : IAsyncProducator<T1>
    {
        public bool TryWrite(T1 value) => target.TryWrite(selector(value));

        public bool TryComplete(Exception? ex = null) => target.TryComplete(ex);

        public bool IsFull => target.IsFull;

        public ValueTask<bool> WaitToWriteAsync() =>
            target is IAsyncProducator<T2> asyncTarget
                ? asyncTarget.WaitToWriteAsync()
                : ValueTask.FromResult(!target.IsFull);
    }

    #endregion

    #region Pumps (pull -> push)

    internal static async Task PumpSync<T1, T2>(IConsumator<T1> reader, IProducator<T2> target, Func<T1, T2> selector, CancellationToken token)
    {
        try
        {
            while (!token.IsCancellationRequested)
            {
                if (reader.TryRead(out var item))
                {
                    var mapped = selector(item);

                    while (!token.IsCancellationRequested)
                    {
                        if (target.TryWrite(mapped))
                            break;

                        await Task.Yield();
                    }

                    continue;
                }

                if (reader.IsCompleted)
                    break;

                await Task.Yield();
            }
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

    internal static async Task PumpAsync<T1, T2>(IAsyncConsumator<T1> reader, IProducator<T2> target, Func<T1, T2> selector, CancellationToken token)
    {
        try
        {
            while (!token.IsCancellationRequested)
            {
                if (reader.TryRead(out var item))
                {
                    var mapped = selector(item);

                    while (!token.IsCancellationRequested)
                    {
                        if (target.TryWrite(mapped))
                            break;

                        await Task.Yield();
                    }

                    continue;
                }

                if (!await reader.WaitToReadAsync())
                    break;
            }
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

    internal static async Task PumpSyncAsync<T1, T2>(IConsumator<T1> reader, IAsyncProducator<T2> target, Func<T1, T2> selector, CancellationToken token)
    {
        try
        {
            while (!token.IsCancellationRequested)
            {
                if (reader.TryRead(out var item))
                {
                    var mapped = selector(item);

                    while (!token.IsCancellationRequested)
                    {
                        if (target.TryWrite(mapped))
                            break;

                        await target.WaitToWriteAsync();
                    }

                    continue;
                }

                if (reader.IsCompleted)
                    break;

                await Task.Yield();
            }
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

    internal static async Task PumpAsyncAsync<T1, T2>(IAsyncConsumator<T1> reader, IAsyncProducator<T2> target, Func<T1, T2> selector, CancellationToken token)
    {
        try
        {
            while (!token.IsCancellationRequested)
            {
                if (reader.TryRead(out var item))
                {
                    var mapped = selector(item);

                    while (!token.IsCancellationRequested)
                    {
                        if (target.TryWrite(mapped))
                            break;

                        await target.WaitToWriteAsync();
                    }

                    continue;
                }

                if (!await reader.WaitToReadAsync())
                    break;
            }
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

    #endregion
}