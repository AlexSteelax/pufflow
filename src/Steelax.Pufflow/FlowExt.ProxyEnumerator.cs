// using Steelax.Pufflow.Abstractions;
//
// namespace Steelax.Pufflow;
//
// public static partial class FlowExt
// {
//     #region Enumerator
//     
//     // batching: IAsyncEnumerator → IEnumerator
//     // embedding a synchronous chain into an asynchronous one
//     [PublicAPI]
//     public static ConfigurablePipe<IAsyncEnumerator<T1>, IEnumerator<T2>, object> Next<T1, T2>(this Source<IAsyncEnumerator<T1>> left, IFlowable<Pipe<IEnumerator<T1>, IEnumerator<T2>>> right)
//         => throw new NotImplementedException();
//     
//     // batching: IAsyncEnumerator → IConsumator
//     // embedding a synchronous chain (compatible) into an asynchronous one
//     [PublicAPI]
//     public static ConfigurablePipe<IAsyncEnumerator<T1>, IConsumator<T2>, object> Next<T1, T2>(this Source<IAsyncEnumerator<T1>> left, IFlowable<Pipe<IEnumerator<T1>, IConsumator<T2>>> right)
//         => throw new NotImplementedException();
//     
//     // batching: IAsyncEnumerator → IAsyncConsumator
//     // embedding an asynchronous chain into an asynchronous chain
//     // note: this effectively builds a processing block that splits the input stream into
//     //       segments, processes them asynchronously, and forwards the results downstream
//     [PublicAPI]
//     public static ConfigurablePipe<IAsyncEnumerator<T1>, IAsyncConsumator<T2>, object> Next<T1, T2>(this Source<IAsyncEnumerator<T1>> left, IFlowable<Pipe<IEnumerator<T1>, IAsyncConsumator<T2>>> right)
//         => throw new NotImplementedException();
//     
//     // batching: IAsyncConsumator → IConsumator
//     // embedding a synchronous chain into an asynchronous one
//     [PublicAPI]
//     public static ConfigurablePipe<IAsyncConsumator<T1>, IConsumator<T2>, object> Next<T1, T2>(this Source<IAsyncConsumator<T1>> left, IFlowable<Pipe<IConsumator<T1>, IConsumator<T2>>> right)
//         => throw new NotImplementedException();
//     
//     // batching: IAsyncConsumator → IEnumerator
//     // embedding a synchronous chain (compatible) into an asynchronous one
//     [PublicAPI]
//     public static ConfigurablePipe<IAsyncConsumator<T1>, IEnumerator<T2>, object> Next<T1, T2>(this Source<IAsyncConsumator<T1>> left, IFlowable<Pipe<IConsumator<T1>, IEnumerator<T2>>> right)
//         => throw new NotImplementedException();
//     
//     // batching: IAsyncConsumator → IAsyncEnumerator
//     // embedding an asynchronous chain into an asynchronous chain (compatible)
//     // note: similar to Next<T1, T2>(this Source<IAsyncEnumerator<T1>> left, IFlowable<Pipe<IEnumerator<T1>, IAsyncConsumator<T2>>> right)
//     [PublicAPI]
//     public static ConfigurablePipe<IAsyncConsumator<T1>, IAsyncEnumerator<T2>, object> Next<T1, T2>(this Source<IAsyncConsumator<T1>> left, IFlowable<Pipe<IConsumator<T1>, IAsyncEnumerator<T2>>> right)
//         => throw new NotImplementedException();
//     
//     #endregion
// }
