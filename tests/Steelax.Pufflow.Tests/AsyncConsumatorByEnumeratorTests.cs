using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using Steelax.Pufflow.Abstractions;
using Steelax.Pufflow.Adapters;
// ReSharper disable AccessToDisposedClosure

namespace Steelax.Pufflow.Tests;

public class AsyncConsumatorByEnumeratorTests
{
    private static (ChannelWriter<int> Channel, AsyncConsumatorByEnumerator<int> Consumator) Create(CancellationToken token)
    {
        var channel = Channel.CreateUnbounded<int>();
        var enumerator = channel.Reader
            .ReadAllAsync(TestContext.Current.CancellationToken)
            .GetAsyncEnumerator(TestContext.Current.CancellationToken);
        var consumator = new AsyncConsumatorByEnumerator<int>(enumerator, token);
        return (channel, consumator);
    }

    /// <summary>
    /// Creates a consumator over a channel whose enumerator observes <paramref name="enumeratorToken"/>,
    /// while the consumator's completion contract observes <paramref name="consumatorToken"/>.
    /// </summary>
    private static AsyncConsumatorByEnumerator<int> CreateFromChannel(Channel<int> channel, CancellationToken enumeratorToken, CancellationToken consumatorToken)
    {
        var enumerator = channel.Reader
            .ReadAllAsync(enumeratorToken)
            .GetAsyncEnumerator(enumeratorToken);

        return new AsyncConsumatorByEnumerator<int>(enumerator, consumatorToken);
    }

    [Fact(Timeout = 1000)]
    public async Task Reads_ReadyValues_AndIsNotCompleted_WhileDataPending()
    {
        var (channel, consumator) = Create(TestContext.Current.CancellationToken);

        Assert.True(channel.TryWrite(1));
        Assert.True(channel.TryWrite(2));
        Assert.True(channel.TryWrite(3));

        var task = Task.Run(() => ConsumeAllAsync(consumator));

        // Values are ready but not drained: the stream is not completed yet.
        Assert.False(consumator.IsCompleted);

        channel.TryComplete();

        var (_, _, result) = await task.WaitAsync(TestContext.Current.CancellationToken);;

        Assert.Equal([1, 2, 3], result);
        Assert.True(consumator.IsCompleted);

        await consumator.DisposeAsync();
    }

    [Theory(Timeout = 1000)]
    [InlineData(5, 0)]
    [InlineData(5, 100)]
    public async Task CompletingSource_ElevatesIsCompleted_AndWaitToReadReturnsFalse(int count, int delay)
    {
        var (channel, consumator) = Create(TestContext.Current.CancellationToken);

        var write = Task.Run(() => WriteManyAsync(channel, count, delay), TestContext.Current.CancellationToken);
        var read = Task.Run(() => ConsumeAllAsync(consumator));

        var (spins, waiters, result) = await read.WaitAsync(TestContext.Current.CancellationToken);
        
        await write.WaitAsync(TestContext.Current.CancellationToken);

        Assert.Equal([1, 2, 3, 4, 5], result);
        Assert.True(consumator.IsCompleted);
        Assert.False(await consumator.WaitToReadAsync());

        await consumator.DisposeAsync();
    }

    [FlakyFact(Timeout = 1000)]
    public async Task Cancellation_CompletesStream()
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var (channel, consumator) = Create(cts.Token);

        var task = Task.Run(() => ConsumeAllAsync(consumator), TestContext.Current.CancellationToken);

        Assert.False(task.IsCompleted);
        
        await cts.CancelAsync();
        
        var (spins, waiters, result) = await task.WaitAsync(TestContext.Current.CancellationToken);
        
        Assert.Empty(result);
        Assert.Equal(1, spins);
        Assert.Equal(1, waiters);
        
        channel.TryComplete();

        await consumator.DisposeAsync();
    }

    [FlakyFact(Timeout = 1000)]
    public async Task DisposeAsync_DisposesUnderlyingEnumerator()
    {
        var disposeCount = 0;
        var consumator = new AsyncConsumatorByEnumerator<int>(
            new TrackingEnumerator(() => disposeCount++),
            TestContext.Current.CancellationToken);

        await consumator.DisposeAsync();

        Assert.Equal(1, disposeCount);
        Assert.True(consumator.IsCompleted);
    }

    [FlakyFact(Timeout = 1000)]
    public async Task DisposeAsync_WithInFlightRead_SettlesWhenSourceCompletes()
    {
        var (channel, consumator) = Create(TestContext.Current.CancellationToken);

        var task = Task.Run(() => ConsumeAllAsync(consumator));

        channel.TryComplete();

        await consumator.DisposeAsync();

        var (_, _, result) = await task.WaitAsync(TestContext.Current.CancellationToken);

        Assert.Empty(result);
        Assert.True(consumator.IsCompleted);
    }

    [Fact]
    public async Task EmptySource_CompletesImmediately_AndWaitToReadReturnsFalse()
    {
        var channel = Channel.CreateUnbounded<int>();
        channel.Writer.TryComplete();

        var consumator = CreateFromChannel(channel, TestContext.Current.CancellationToken, TestContext.Current.CancellationToken);

        Assert.False(consumator.TryRead(out _));
        Assert.True(consumator.IsCompleted);
        Assert.False(await consumator.WaitToReadAsync());
    }

    [Fact]
    public void TryRead_WhileStreamOpenAndEmpty_ReturnsFalse()
    {
        var (_, consumator) = Create(TestContext.Current.CancellationToken);

        Assert.False(consumator.TryRead(out _));
        Assert.False(consumator.IsCompleted);
    }

    [Fact]
    public void SourceFault_ThrowsOnRead()
    {
        var error = new InvalidOperationException("Source failed");
        var consumator = new AsyncConsumatorByEnumerator<int>(
            new FaultingEnumerator(error),
            TestContext.Current.CancellationToken);

        Assert.Same(error, Assert.Throws<InvalidOperationException>(() => consumator.TryRead(out _)));
    }

    [Fact]
    public async Task SourceFaultWithCancellation_TreatedAsCompletion()
    {
        var consumator = new AsyncConsumatorByEnumerator<int>(
            new FaultingEnumerator(new OperationCanceledException()),
            TestContext.Current.CancellationToken);

        Assert.False(consumator.TryRead(out _));
        Assert.True(consumator.IsCompleted);
        Assert.False(await consumator.WaitToReadAsync());
    }

    [FlakyFact(Timeout = 1000)]
    public async Task CanceledEnumeratorMove_CompletesStream_AndWaitToReadReturnsFalse()
    {
        using var cts = new CancellationTokenSource();
        var channel = Channel.CreateUnbounded<int>();
        var consumator = CreateFromChannel(channel, cts.Token, TestContext.Current.CancellationToken);

        var task = Task.Run(() => ConsumeAllAsync(consumator));

        await cts.CancelAsync();

        var (_, _, result) = await task.WaitAsync(TestContext.Current.CancellationToken);

        Assert.Empty(result);
        Assert.True(consumator.IsCompleted);
        Assert.False(await consumator.WaitToReadAsync());

        channel.Writer.TryComplete();
        await consumator.DisposeAsync();
    }

    [SuppressMessage("Performance", "CA1859")]
    private static async Task<(int Spins, int Waiters, IReadOnlyCollection<int> Result)> ConsumeAllAsync(IAsyncConsumator<int> consumator)
    {
        var result = new List<int>();
        var spins = 0;
        var waiters = 0;

        while (true)
        {
            spins++;

            if (consumator.TryRead(out var value))
            {
                result.Add(value);
                continue;
            }

            waiters++;

            if (!await consumator.WaitToReadAsync())
                break;
        }

        return (spins, waiters, result);
    }

    private static async Task WriteManyAsync(ChannelWriter<int> writer, int count, int delay)
    {
        var timer = delay > 0
            ? new PeriodicTimer(TimeSpan.FromMilliseconds(delay))
            : null;
        
        try
        {
            foreach (var value in Enumerable.Range(1, count))
            {
                if (timer is not null)
                    await timer.WaitForNextTickAsync();

                writer.TryWrite(value);
            }
        }
        finally
        {
            timer?.Dispose();
            writer.TryComplete();
        }
    }

    /// <summary>
    /// An enumerator that immediately ends the stream and tracks disposal.
    /// </summary>
    private sealed class TrackingEnumerator(Action onDispose) : IAsyncEnumerator<int>
    {
        public int Current => 0;

        public ValueTask<bool> MoveNextAsync() => ValueTask.FromResult(false);

        public ValueTask DisposeAsync()
        {
            onDispose.Invoke();
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>
    /// An enumerator whose move faults with the supplied exception.
    /// </summary>
    private sealed class FaultingEnumerator(Exception error) : IAsyncEnumerator<int>
    {
        public int Current => default;

        public ValueTask<bool> MoveNextAsync() => ValueTask.FromException<bool>(error);

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
