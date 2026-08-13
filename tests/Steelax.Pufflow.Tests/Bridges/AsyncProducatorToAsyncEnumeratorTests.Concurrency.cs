// using Steelax.Pufflow.Bridges;
//
// namespace Steelax.Pufflow.Tests.Bridges;
//
// public static partial class AsyncProducatorToAsyncEnumeratorTests
// {
//     public sealed class Concurrency
//     {
//         [Fact(Timeout = 1000)]
//         public async Task ConcurrentProducerConsumer_AllDeliveredInOrder()
//         {
//             const int count = 10_000;
//             const int limit = 16;
//             var bridge = new AsyncProducatorToAsyncEnumerator<int>(limit);
//
//             var producer = Task.Run(async () =>
//             {
//                 for (var i = 0; i < count; i++)
//                     while (!bridge.TryWrite(i))
//                         await bridge.WaitToWriteAsync();
//
//                 bridge.TryComplete();
//             }, TestContext.Current.CancellationToken);
//
//             var collected = new List<int>(count);
//             while (await bridge.MoveNextAsync())
//                 collected.Add(bridge.Current);
//
//             await producer;
//
//             Assert.Equal(count, collected.Count);
//             Assert.Equal(Enumerable.Range(0, count), collected);
//         }
//
//         [Fact(Timeout = 1000)]
//         public async Task ConcurrentProducerConsumer_SmallLimit_NoLoss()
//         {
//             const int count = 5_000;
//             var bridge = new AsyncProducatorToAsyncEnumerator<int>(1);
//
//             var producer = Task.Run(async () =>
//             {
//                 for (var i = 0; i < count; i++)
//                     while (!bridge.TryWrite(i))
//                         await bridge.WaitToWriteAsync();
//
//                 bridge.TryComplete();
//             }, TestContext.Current.CancellationToken);
//
//             // With a limit of 1 the producer blocks on every slot — the tightest backpressure path.
//             var read = 0;
//             long sum = 0;
//
//             while (await bridge.MoveNextAsync())
//             {
//                 sum += bridge.Current;
//                 read++;
//             }
//
//             await producer;
//
//             Assert.Equal(count, read);
//             Assert.Equal((long)count * (count - 1) / 2, sum);
//         }
//
//         [Fact(Timeout = 1000)]
//         public async Task ConcurrentProducerConsumer_ManyRounds_Stable()
//         {
//             const int rounds = 50;
//             const int count = 5000;
//
//             for (var round = 0; round < rounds; round++)
//             {
//                 var bridge = new AsyncProducatorToAsyncEnumerator<int>(8);
//
//                 var producer = Task.Run(async () =>
//                 {
//                     for (var i = 0; i < count; i++)
//                         while (!bridge.TryWrite(i))
//                             await bridge.WaitToWriteAsync();
//     
//                     bridge.TryComplete();
//                 }, TestContext.Current.CancellationToken);
//
//                 var read = 0;
//                 while (await bridge.MoveNextAsync())
//                     read++;
//
//                 await producer;
//
//                 Assert.Equal(count, read);
//             }
//         }
//
//         [Fact(Timeout = 1000)]
//         public async Task ConcurrentFault_RethrownOnConsumer()
//         {
//             var bridge = new AsyncProducatorToAsyncEnumerator<int>(4);
//             var ex = new InvalidOperationException("producer failed");
//
//             // Consumer waits for data that never comes; the producer faults the bridge.
//             var consumer = Task.Run(async () => await bridge.MoveNextAsync(), TestContext.Current.CancellationToken);
//             await Task.Delay(50, TestContext.Current.CancellationToken);
//
//             bridge.TryComplete(ex);
//
//             var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () => await consumer);
//             Assert.Same(ex, thrown);
//         }
//
//         [Fact(Timeout = 1000)]
//         public async Task ConcurrentEmptyComplete_ConsumerSeesEndOfStream()
//         {
//             var bridge = new AsyncProducatorToAsyncEnumerator<int>(4);
//
//             // Consumer waits for data; the producer completes the stream with nothing written.
//             var consumer = Task.Run(async () => await bridge.MoveNextAsync(), TestContext.Current.CancellationToken);
//             await Task.Delay(50, TestContext.Current.CancellationToken);
//
//             bridge.TryComplete();
//
//             Assert.False(await consumer);
//         }
//     }
// }