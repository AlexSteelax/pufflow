using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Sdk.Test;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Buffering;

public static partial class BypassBufferProcessorTests
{
    /// <summary>
    ///     Carrier-envelope integrity through <c>Buffering(capacity)</c>: the buffer is a passive relay, so a
    ///     <see cref="Carrier{T}" /> must arrive downstream byte-for-byte identical — value, attached watermark,
    ///     and <c>HasValue</c> flag. In particular the two "empty-adjacent" watermark states must not be confused:
    ///     <c>Watermark.Nothing()</c> is <c>-1</c> while <c>default(Watermark)</c>/<c>Watermark.From(0)</c> is <c>0</c>.
    /// </summary>
    public sealed class CarrierEnvelope
    {
        private const int TimeoutMs = 2_000;

        private sealed record Snapshot(int? Value, long Watermark, bool HasValue);

        private static Snapshot Of(Carrier<int> item) =>
            item.HasValue ? new Snapshot(item.Value, item.Watermark, HasValue: true) : new Snapshot(null, item.Watermark, HasValue: false);

        private static async Task<List<Snapshot>> RunAsync(
            IEnumerable<Carrier<int>> input,
            int capacity,
            FlowSource flow,
            CancellationToken cancellationToken)
        {
            flow
                .OnAsyncProducatorSource(input)
                .Buffering(capacity)
                .Consume(out var reader);

            await flow.ExecuteAsync(cancellationToken);

            var result = new List<Snapshot>();
            await foreach (var item in reader.ReadAllAsync(TestContext.Current.CancellationToken))
                result.Add(Of(item));

            return result;
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task WatermarksAndValues_PassThroughUnchanged()
        {
            var input = new Carrier<int>[]
            {
                new(1, Watermark.From(10)),
                new(2, Watermark.From(20)),
                new Carrier<int>(Watermark.From(30)), // bare progress
                new(3, Watermark.From(25)),
                new Carrier<int>(Watermark.From(40)),
            };

            var expected = input.Select(Of).ToList();

            await using var flow = new FlowSource();
            var actual = await RunAsync(input, capacity: 3, flow, TestContext.Current.CancellationToken);

            Assert.Equal(expected, actual);
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task NothingWatermark_StaysNothing_NotZero()
        {
            // Watermark.Nothing() is the -1 sentinel. The buffer must relay it as-is; a corrupted read (default
            // slot, ignored TryRead bool) would surface as watermark 0 instead.
            var input = new Carrier<int>[]
            {
                new Carrier<int>(Watermark.Nothing()),
                new Carrier<int>(Watermark.Nothing()),
            };

            await using var flow = new FlowSource();
            var actual = await RunAsync(input, capacity: 2, flow, TestContext.Current.CancellationToken);

            Assert.Equal(2, actual.Count);
            Assert.All(actual, static i => Assert.Equal(-1, i.Watermark));
            Assert.All(actual, static i => Assert.False(i.HasValue));

            Assert.DoesNotContain(actual, static i => i.Watermark == 0);
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task ZeroInputWatermark_StaysZero_NotTurnedIntoNothing()
        {
            // Watermark.From(0) is a legitimate value (default(Watermark)) distinct from the -1 Nothing sentinel.
            var input = new[]
            {
                new Carrier<int>(5, Watermark.From(0)),
                new Carrier<int>(watermark: Watermark.From(0)),
            };

            await using var flow = new FlowSource();
            var actual = await RunAsync(input, capacity: 2, flow, TestContext.Current.CancellationToken);

            Assert.Equal(2, actual.Count);
            Assert.Equal(new Snapshot(5, 0, HasValue: true), actual[0]);
            Assert.Equal(new Snapshot(null, 0, HasValue: false), actual[1]);
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task RingWrapAround_NoPhantomDefaultSlots()
        {
            // More items than the ring capacity (capacity 1 → 2 slots) force the reader to wrap around its
            // sequence. Every read must return exactly one written item; a failed read must not leak a
            // default(Carrier) whose watermark would read as 0.
            var input = new Carrier<int>[]
            {
                new(1, Watermark.From(10)),
                new Carrier<int>(Watermark.Nothing()),
                new(2, Watermark.From(20)),
                new(3, Watermark.From(30)),
                new Carrier<int>(Watermark.From(0)),
                new(4, Watermark.From(40)),
            };

            var expected = input.Select(Of).ToList();

            await using var flow = new FlowSource();
            var actual = await RunAsync(input, capacity: 1, flow, TestContext.Current.CancellationToken);

            Assert.Equal(expected, actual);
            Assert.Equal(input.Length, actual.Count);
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task Backpressure_ManyCarriers_AllDeliveredIntact()
        {
            // A long stream at capacity 2: the writer applies backpressure repeatedly; every carrier must still
            // arrive once, in order, with its watermark untouched.
            var input = Enumerable.Range(0, 100)
                .Select<int, Carrier<int>>(i => i % 3 == 0
                    ? new Carrier<int>(i, Watermark.Nothing())
                    : new Carrier<int>(i, Watermark.From(i * 10)))
                .ToArray();

            var expected = input.Select(Of).ToList();

            await using var flow = new FlowSource();
            var actual = await RunAsync(input, capacity: 2, flow, TestContext.Current.CancellationToken);

            Assert.Equal(expected, actual);
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task EmptySource_YieldsNoPhantomCarriers()
        {
            await using var flow = new FlowSource();
            var actual = await RunAsync([], capacity: 2, flow, TestContext.Current.CancellationToken);

            Assert.Empty(actual);
        }
    }
}