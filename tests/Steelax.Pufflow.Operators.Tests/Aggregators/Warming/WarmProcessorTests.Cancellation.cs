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
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var job = new WarmingHelper.TcsJob(tcs);
            var policy = new WarmingHelper.PredicatePolicy(WarmEvenOnly); // warms key 2

            var flow = new FlowSource();

            var options = new WarmOptions
            {
                MaxConcurrency = 1,
                MaxQueued = 8,
                SegmentCapacity = 4,
                SegmentTtl = TimeSpan.FromMilliseconds(100),
                QueueWeightLimit = 1000,
            };

            flow
                .OnAsyncConsumatorSource([new Carrier<int>(2, Watermark.From(20))])
                .Warming(
                    options,
                    new WarmingHelper.ManualJobFactory(() => job),
                    ValueToKey,
                    policy,
                    new ListAccumulatorFactory())
                .Consume(out var reader);

            // Start the pipeline: background tasks begin (WarmProcessor via Task.Run and sink via
            // RegisterBackground). ExecuteAsync waits for their completion, so keep it in the background.
            var execution = flow.ExecuteAsync(TestContext.Current.CancellationToken);

            // Wait for the warm job to start вЂ” by then the loop is asleep waiting.
            Assert.WaitUntil(() => job.Started, TestContext.Current.CancellationToken);

            // Stop the pipeline in the correct order: cancel the flow token first, let the background
            // tasks unwind on the (still valid) token, wait for ExecuteAsync to finish, and only then
            // dispose the source. Disposing first would tear down the CTS while tasks still observe it.
            flow.Context.Cancel();
            await execution;
            await flow.DisposeAsync();

            // The buffer must be completed (in finally) вЂ” the wait finishes and TryRead reports Completed.
            await reader.WaitToReadAsync(TestContext.Current.CancellationToken);
            Assert.True(reader.Completion.IsCompleted);
        }
    }
}
