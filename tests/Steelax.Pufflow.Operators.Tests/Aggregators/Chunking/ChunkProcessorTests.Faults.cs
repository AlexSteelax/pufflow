using System.Threading.Channels;
using Steelax.Pufflow.Sdk.Test;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Chunking;

public static partial class ChunkProcessorTests
{
    public sealed class Faults
    {
        [Fact(Timeout = TimeoutMs)]
        public async Task FaultedSource_DeliversAccumulatedChunk_ThenThrows()
        {
            var ex = new InvalidOperationException("source error");

            await using var flow = new FlowSource(TestContext.Current.CancellationToken);
            flow
                .OnAsyncConsumatorSource(out ChannelWriter<int> writer)
                .Chunking(10, TimeSpan.FromSeconds(1))
                .Consume(out var reader);

            var runTask = flow.ExecuteAsync();

            writer.TryWrite(1);
            writer.TryComplete(ex);

            var chunks = new List<int[]>();

            // The accumulated chunk is delivered before the source fault propagates.
            var consume = Task.Run(async () =>
            {
                await foreach (var chunk in reader.ReadAllAsync(TestContext.Current.CancellationToken))
                    using (chunk)
                    {
                        chunks.Add(chunk.Span.ToArray());
                    }
            }, TestContext.Current.CancellationToken);
            
            // The fault propagates through the channel completion.
            var thrown1 = await Assert.ThrowsAsync<InvalidOperationException>(() => consume);
            Assert.Same(ex, thrown1);

            foreach(var chunk in chunks)
                Assert.Equal<int[]>([1], chunk);

            await runTask;
        }
    }
}
