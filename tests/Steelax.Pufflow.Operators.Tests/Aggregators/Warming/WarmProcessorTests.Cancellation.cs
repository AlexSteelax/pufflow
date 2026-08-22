using Steelax.Pufflow.Operators.Aggregators.Warming;
using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Sdk.Test;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Warming;

public static partial class WarmProcessorTests
{
    public sealed class Cancellation
    {
        [Fact(Timeout = 1_000)]
        public async Task CancelWhileJobPending_CompletesBuffer()
        {
            // The warm job never completes: the WarmProcessor loop sleeps on the fan-in waiting for it.
            var job = new TcsJob();
            var policy = new TestPolicy(); // warms key 2

            var flow = new FlowSource();

            var options = new WarmOptions
            {
                MaxConcurrency = 1,
                MaxQueued = 8,
                SegmentCapacity = 4,
                SegmentLinger = TimeSpan.FromMilliseconds(NoLingerMs),
                QueueWeightLimit = 1000
            };

            flow
                .OnAsyncConsumatorSource([new Watermarked<int>(2, Watermark.From(20))])
                .Warming(
                    options,
                    new TcsJobFactory(job),
                    ValueToKey,
                    policy,
                    new ListAccumulatorFactory())
                .Consume(out var reader);

            // Start the pipeline: background tasks begin (WarmProcessor via Task.Run and sink via
            // RegisterBackground). ExecuteAsync waits for their completion, so keep it in the background.
            var execution = flow.ExecuteAsync(TestContext.Current.CancellationToken);

            // Wait for the warm job to start — by then the loop is asleep waiting.
            await WaitUntilAsync(() => job.Started, flow.Context.Token);

            await flow.DisposeAsync();

            // The buffer must be completed (in finally) — the wait finishes and TryRead reports Completed.
            await reader.WaitToReadAsync(TestContext.Current.CancellationToken);
            Assert.True(reader.Completion.IsCompleted);

            await execution;
        }
    }
}