// using Steelax.Pufflow.Tests.Flows.Push;
// using static Steelax.Pufflow.Tests.TestFlowExt.Transform;
// using static Steelax.Pufflow.Tests.TestFlowExt.Source;
//
// namespace Steelax.Pufflow.Tests;
//
// public class DirectFlowTests
// {
//     private static readonly int[] Items = [1, 2, 3];
//     private static T Passthrough<T>(T input) => input;
//
//     public class Enumerator
//     {
//         [Fact]
//         public void SyncToSync()
//         {
//             var enumerator = FlowExt
//                 .Next(new Enumerator<int>(Items))
//                 .Next(new Enumerator<int, int>(Passthrough))
//                 .Build();
//
//             var items = new List<int>();
//             while (enumerator.MoveNext())
//                 items.Add(enumerator.Current);
//             
//             Assert.Equal(Items, items);
//         }
//
//         [Fact]
//         public void AsyncToAsync()
//         {
//             Assert.Throws<NotImplementedException>(() => FlowExt
//                 .Next(new AsyncEnumerator<int>(Items))
//                 .Next(new AsyncEnumerator<int, int>(Passthrough)));
//         }
//
//         [Fact]
//         public void SyncToAsync()
//         {
//             Assert.Throws<NotImplementedException>(() => FlowExt
//                 .Next(new Enumerator<int>(Items))
//                 .Next(new EnumeratorToAsyncEnumerator<int, int>(Passthrough)));
//         }
//
//         [Fact]
//         public void SyncToConsumator()
//         {
//             Assert.Throws<NotImplementedException>(() => FlowExt
//                 .Next(new Enumerator<int>(Items))
//                 .Next(new EnumeratorToConsumator<int, int>(Passthrough)));
//         }
//
//         [Fact]
//         public void SyncToAsyncConsumator()
//         {
//             Assert.Throws<NotImplementedException>(() => FlowExt
//                 .Next(new Enumerator<int>(Items))
//                 .Next(new EnumeratorToAsyncConsumator<int, int>(Passthrough)));
//         }
//
//         [Fact]
//         public void AsyncToAsyncConsumator()
//         {
//             Assert.Throws<NotImplementedException>(() => FlowExt
//                 .Next(new AsyncEnumerator<int>(Items))
//                 .Next(new AsyncEnumeratorToAsyncConsumator<int, int>(Passthrough)));
//         }
//     }
//
//     public class Consumator
//     {
//         [Fact]
//         public void SyncToSync()
//         {
//             Assert.Throws<NotImplementedException>(() => FlowExt
//                 .Next(new Consumator<int>(Items))
//                 .Next(new Consumator<int, int>(Passthrough)));
//         }
//
//         [Fact]
//         public void AsyncToAsync()
//         {
//             Assert.Throws<NotImplementedException>(() => FlowExt
//                 .Next(new AsyncConsumator<int>(Items))
//                 .Next(new AsyncConsumator<int, int>(Passthrough)));
//         }
//
//         [Fact]
//         public void SyncToAsync()
//         {
//             Assert.Throws<NotImplementedException>(() => FlowExt
//                 .Next(new Consumator<int>(Items))
//                 .Next(new ConsumatorToAsyncConsumator<int, int>(Passthrough)));
//         }
//
//         [Fact]
//         public void SyncToEnumerator()
//         {
//             Assert.Throws<NotImplementedException>(() => FlowExt
//                 .Next(new Consumator<int>(Items))
//                 .Next(new ConsumatorToEnumerator<int, int>(Passthrough)));
//         }
//
//         [Fact]
//         public void SyncToAsyncEnumerator()
//         {
//             Assert.Throws<NotImplementedException>(() => FlowExt
//                 .Next(new Consumator<int>(Items))
//                 .Next(new ConsumatorToAsyncEnumerator<int, int>(Passthrough)));
//         }
//
//         [Fact]
//         public void AsyncToAsyncEnumerator()
//         {
//             Assert.Throws<NotImplementedException>(() => FlowExt
//                 .Next(new AsyncConsumator<int>(Items))
//                 .Next(new AsyncConsumatorToAsyncEnumerator<int, int>(Passthrough)));
//         }
//     }
//
//     public class Producator
//     {
//         [Fact]
//         public void SyncToSync()
//         {
//             Assert.Throws<NotImplementedException>(() => FlowExt
//                 .Next(new SourceFlowProducator<int>())
//                 .Next(new PipeFlowProducator<int, int>()));
//         }
//     }
// }

