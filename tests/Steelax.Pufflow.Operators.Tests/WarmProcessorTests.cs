using Unio;

namespace Steelax.Pufflow.Operators.Tests;

/// <summary>
///     Black-box tests for <see cref="WarmProcessor{TKey,TValue,TGroup,TWarm}" />: a source enumerator is
///     pushed through the processor and the resulting <see cref="Unio{T0,T1,T2}" /> stream is observed.
/// </summary>
public static partial class WarmProcessorTests
{
    private const int NoLingerMs = 60_000;

    private static Warmer<int, string> CreateWarmer(IJobFactory<int, string> factory)
    {
        return new Warmer<int, string>(
            1,
            8,
            4,
            TimeSpan.FromMilliseconds(NoLingerMs),
            factory);
    }

    private static WarmProcessor<int, int, string, string> CreateProcessor(
        Warmer<int, string> warmer,
        TestPolicy policy,
        IWarmAccumulatorFactory<int, int, string> accumulatorFactory,
        TimeSpan? watchdogPeriod = null)
    {
        return new WarmProcessor<int, int, string, string>(warmer, static (in v) => v, policy, accumulatorFactory, 1000,
            watchdogPeriod ?? Timeout.InfiniteTimeSpan);
    }

    private static async Task<List<Unio<int, string, Watermark>>> RunAsync(
        Warmer<int, string> warmer,
        TestPolicy policy,
        IWarmAccumulatorFactory<int, int, string> accumulatorFactory,
        IReadOnlyList<Watermarked<int>> input,
        FlowContext context,
        TimeSpan? watchdogPeriod = null)
    {
        var processor = CreateProcessor(warmer, policy, accumulatorFactory, watchdogPeriod);
        var output = processor.GetAsyncConsumator(new ListAsyncEnumerator<Watermarked<int>>(input), context);

        var results = new List<Unio<int, string, Watermark>>();
        while (true)
            if (output.TryRead(out var item, out var completed))
            {
                results.Add(item);
            }
            else
            {
                if (completed)
                    break;

                await output.WaitToReadAsync();
            }

        return results;
    }

    private static async Task WaitUntilAsync(Func<bool> condition, CancellationToken token)
    {
        while (!condition() && !token.IsCancellationRequested)
            await Task.Delay(10, token);
    }

    // Источник: простой IAsyncEnumerator над готовым списком.
    private sealed class ListAsyncEnumerator<T>(IReadOnlyList<T> items) : IAsyncEnumerator<T>
    {
        private int _index = -1;

        public T Current => items[_index];

        public ValueTask<bool> MoveNextAsync()
        {
            return ValueTask.FromResult(++_index < items.Count);
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    // Аккумулятор: копит int-значения и отдаёт одну группу-строку при потреблении.
    private sealed class ListAccumulator : WarmAccumulator<int, string>
    {
        private readonly List<int> _values = new();
        private int _consumed;

        protected override void Add(int value)
        {
            _values.Add(value);
        }

        protected override bool TryConsume(out string group, out int weight)
        {
            if (_consumed >= _values.Count)
            {
                group = string.Empty;
                weight = 0;
                return false;
            }

            group = string.Join(",", _values.Skip(_consumed));
            weight = _values.Count - _consumed;
            _consumed = _values.Count;
            return true;
        }
    }

    private sealed class ListAccumulatorFactory : IWarmAccumulatorFactory<int, int, string>
    {
        public WarmAccumulator<int, string> Create(int key)
        {
            return new ListAccumulator();
        }
    }

    // Политика: по умолчанию греем чётные ключи; OnWarmed собирает результаты.
    private sealed class TestPolicy : IWarmPolicy<int, string>
    {
        public bool WarmEvenOnly { get; } = true;

        public List<(int Key, string Warm)> Warmed { get; } = new();

        public bool ShouldWarm(int key)
        {
            return WarmEvenOnly ? key % 2 == 0 : key % 2 != 0;
        }

        public void OnWarmed(int key, string warm)
        {
            Warmed.Add((key, warm));
        }
    }

    // Джоб: мгновенный warm «W» + key.
    private sealed class SyncJob : IAsyncJob<int, string>
    {
        private KeyValuePair<int, string>[] _results = [];

        public Task ExecuteAsync(int[] keys, CancellationToken cancellationToken)
        {
            _results = keys.Select(k => new KeyValuePair<int, string>(k, "W" + k)).ToArray();
            return Task.CompletedTask;
        }

        public ReadOnlySpan<KeyValuePair<int, string>> GetResult()
        {
            return _results.AsSpan();
        }

        public void SynchronousComplete()
        {
        }

        public void Dispose()
        {
        }
    }

    private sealed class SyncJobFactory : IJobFactory<int, string>
    {
        public IAsyncJob<int, string> CreateAsyncJob()
        {
            return new SyncJob();
        }
    }

    // Джоб с задержкой: эмулирует «реальную» тёплую задачу, выполняемую некоторое время.
    private sealed class DelayedJob(int delayMs) : IAsyncJob<int, string>
    {
        private KeyValuePair<int, string>[] _results = Array.Empty<KeyValuePair<int, string>>();

        public async Task ExecuteAsync(int[] keys, CancellationToken cancellationToken)
        {
            await Task.Delay(delayMs, cancellationToken);
            _results = keys.Select(k => new KeyValuePair<int, string>(k, "W" + k)).ToArray();
        }

        public ReadOnlySpan<KeyValuePair<int, string>> GetResult()
        {
            return _results.AsSpan();
        }

        public void SynchronousComplete()
        {
        }

        public void Dispose()
        {
        }
    }

    private sealed class DelayedJobFactory(int delayMs) : IJobFactory<int, string>
    {
        public IAsyncJob<int, string> CreateAsyncJob()
        {
            return new DelayedJob(delayMs);
        }
    }

    // Джоб с управляемым завершением (детерминизм для теста отмены).
    private sealed class TcsJob : IAsyncJob<int, string>
    {
        public TaskCompletionSource Tcs { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int[]? Keys { get; private set; }

        public CancellationToken CancellationToken { get; private set; }

        public bool Started { get; private set; }

        public Task ExecuteAsync(int[] keys, CancellationToken cancellationToken)
        {
            Keys = keys;
            CancellationToken = cancellationToken;
            Started = true;
            return Tcs.Task;
        }

        public ReadOnlySpan<KeyValuePair<int, string>> GetResult()
        {
            return Keys!.Select(k => new KeyValuePair<int, string>(k, "W" + k)).ToArray().AsSpan();
        }

        public void SynchronousComplete()
        {
        }

        public void Dispose()
        {
        }
    }

    private sealed class TcsJobFactory(TcsJob job) : IJobFactory<int, string>
    {
        public IAsyncJob<int, string> CreateAsyncJob()
        {
            return job;
        }
    }
}