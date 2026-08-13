// namespace Steelax.Pufflow.Operators.Tests.Aggregators.Buffering;
//
// public static partial class BufferProcessorTests
// {
//     public sealed class Faults
//     {
//         [Fact]
//         public async Task FaultedSource_Throws()
//         {
//             var ex = new InvalidOperationException("source error");
//             var processor = new Operators.Aggregators.Buffering.BufferProcessor<int>(4);
//
//             await using var sourceEnumerator =
//                 FaultySourceAsync(ex).GetAsyncEnumerator(TestContext.Current.CancellationToken);
//             await using var enumerator = processor.GetAsyncEnumerator(sourceEnumerator, default);
//
//             var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
//             {
//                 while (await enumerator.MoveNextAsync())
//                 {
//                     // drain
//                 }
//             });
//
//             Assert.Same(ex, thrown);
//         }
//     }
// }