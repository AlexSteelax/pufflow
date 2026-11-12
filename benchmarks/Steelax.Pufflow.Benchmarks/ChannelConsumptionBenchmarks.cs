using System.Threading.Channels;
using BenchmarkDotNet.Attributes;
using Steelax.Pufflow.Adapters;

namespace Steelax.Pufflow.Benchmarks;

/// <summary>
///     Compares the throughput of consuming a bounded channel (capacity <see cref="ChannelCapacity" />,
///     single writer, single reader) across three consumption shapes. The producer writes all
///     <see cref="ItemsPerInvoke" /> items on a dedicated background task started in <see cref="Setup" />
///     (outside the measurement), so the measured method is purely the consumer side; the producer is kept
///     throttled by the channel's capacity, exactly like a real streaming feed.
/// </summary>
/// <remarks>
///     <see cref="ChannelForeach" /> and <see cref="ChannelLoop" /> are the two raw baseline shapes; the
///     difference between them shows the cost of the <c>ReadAllAsync</c> enumerator machinery. The interesting
///     line is <see cref="Wrapper" /> — it measures the same raw channel
///     <see cref="IAsyncConsumator{T}" /> pattern wrapped through
///     <see cref="AsyncConsumatorByEnumerator{T}" />, the bridge the generated <c>[Flow]</c> chain uses for
///     <c>EnumeratorChain</c> inputs.
/// </remarks>
[MemoryDiagnoser]
[IterationCount(7)]
[WarmupCount(3)]
[RunOncePerIteration]
public class ChannelConsumptionBenchmarks
{
    private const int ItemsPerInvoke = 10_000_000;
    private const int ChannelCapacity = 512;

    private Channel<int> _channel = null!;
    private Task _producer = null!;

    [BenchmarkCancellation]
    public CancellationToken CancellationToken { get; set; }

    /// <summary>
    ///     Baseline: idiomatic <c>foreach</c> over <see cref="ChannelReader{T}.ReadAllAsync" />. The compiler
    ///     drives a struct enumerator directly over the channel's internal state.
    /// </summary>
    [Benchmark(OperationsPerInvoke = ItemsPerInvoke)]
    public async Task<int> ChannelForeach()
    {
        var sum = 0;

        await foreach (var item in _channel.Reader.ReadAllAsync(CancellationToken))
            sum += item;

        return sum;
    }

    /// <summary>
    ///     Baseline: the same <see cref="IAsyncConsumator{T}" /> pull pattern the wrapper uses
    ///     (<c>TryRead</c> + <c>WaitToReadAsync</c>), applied to the <see cref="ChannelReader{T}" /> directly.
    /// </summary>
    [Benchmark(OperationsPerInvoke = ItemsPerInvoke)]
    public async Task<int> ChannelLoop()
    {
        var reader = _channel.Reader;
        var sum = 0;

        while (true)
        {
            if (reader.TryRead(out var item))
            {
                sum += item;
                continue;
            }

            if (!await reader.WaitToReadAsync(CancellationToken))
                break;
        }

        return sum;
    }

    /// <summary>
    ///     The wrapper: bridges <c>ReadAllAsync</c>'s enumerator through
    ///     <see cref="AsyncConsumatorByEnumerator{T}" /> and consumes it with the same pulled loop. Mirrors
    ///     the generated <c>[Flow]</c> bridge <c>EnumeratorChain&lt;T&gt; → AsyncConsumatorByEnumerator&lt;T&gt;</c>.
    /// </summary>
    [Benchmark(OperationsPerInvoke = ItemsPerInvoke)]
    public async Task<int> Wrapper()
    {
        await using var consumator = new AsyncConsumatorByEnumerator<int>(
            _channel.Reader.ReadAllAsync(CancellationToken).GetAsyncEnumerator(CancellationToken),
            CancellationToken);

        var sum = 0;

        while (true)
        {
            if (consumator.TryRead(out var item))
            {
                sum += item;
                continue;
            }

            if (!await consumator.WaitToReadAsync())
                break;
        }

        return sum;
    }

    /// <summary>Starts a background producer writing <see cref="ItemsPerInvoke" /> items into a fresh bounded channel.</summary>
    [IterationSetup]
    public void Setup()
    {
        _channel = Channel.CreateBounded<int>(new BoundedChannelOptions(ChannelCapacity)
        {
            SingleWriter = true,
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait
        });

        _producer = Task.Run(() => ProduceAsync(_channel, ItemsPerInvoke, CancellationToken), CancellationToken);
    }

    /// <summary>Joins the producer. Once the consumer loop has observed end-of-stream, the producer is done.</summary>
    [IterationCleanup]
    public async Task Cleanup()
    {
        await _producer;
    }

    private static async Task ProduceAsync(Channel<int> channel, int count, CancellationToken cancellationToken)
    {
        var writer = channel.Writer;

        try
        {
            for (var i = 0; i < count; i++)
                await writer.WriteAsync(i, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            // The iteration token is cancelled once the run finished; the channel must still complete so a
            // straggler consumer does not hang on an empty, open channel.
        }
        finally
        {
            writer.TryComplete();
        }
    }
}
