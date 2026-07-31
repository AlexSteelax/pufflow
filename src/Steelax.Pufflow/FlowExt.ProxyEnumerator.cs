// using Steelax.Pufflow.Abstractions;
//
// namespace Steelax.Pufflow;
//
// public static partial class FlowExt
// {
//     #region Enumerator
//     
//     // batching: IAsyncEnumerator → IEnumerator
//     // внедрения синхронной цепочки в асинхронную
//     [PublicAPI]
//     public static ConfigurablePipe<IAsyncEnumerator<T1>, IEnumerator<T2>, object> Next<T1, T2>(this Source<IAsyncEnumerator<T1>> left, IFlowable<Pipe<IEnumerator<T1>, IEnumerator<T2>>> right)
//         => throw new NotImplementedException();
//     
//     // batching: IAsyncEnumerator → IConsumator
//     // внедрения синхронной цепочки (совместимое) в асинхронную
//     [PublicAPI]
//     public static ConfigurablePipe<IAsyncEnumerator<T1>, IConsumator<T2>, object> Next<T1, T2>(this Source<IAsyncEnumerator<T1>> left, IFlowable<Pipe<IEnumerator<T1>, IConsumator<T2>>> right)
//         => throw new NotImplementedException();
//     
//     // batching: IAsyncEnumerator → IAsyncConsumator
//     // внедрение асинхронной цепочки в асинхронную цепочку
//     // примечание: по сути это организиет блок обработки с дроблением входного потока на сегменты с асинхронной обработкой этих сегментов и возврата результата дальше по потоку
//     [PublicAPI]
//     public static ConfigurablePipe<IAsyncEnumerator<T1>, IAsyncConsumator<T2>, object> Next<T1, T2>(this Source<IAsyncEnumerator<T1>> left, IFlowable<Pipe<IEnumerator<T1>, IAsyncConsumator<T2>>> right)
//         => throw new NotImplementedException();
//     
//     // batching: IAsyncConsumator → IConsumator
//     // внедрения синхронной цепочки в асинхронную
//     [PublicAPI]
//     public static ConfigurablePipe<IAsyncConsumator<T1>, IConsumator<T2>, object> Next<T1, T2>(this Source<IAsyncConsumator<T1>> left, IFlowable<Pipe<IConsumator<T1>, IConsumator<T2>>> right)
//         => throw new NotImplementedException();
//     
//     // batching: IAsyncConsumator → IEnumerator
//     // внедрения синхронной цепочки (совместимое) в асинхронную
//     [PublicAPI]
//     public static ConfigurablePipe<IAsyncConsumator<T1>, IEnumerator<T2>, object> Next<T1, T2>(this Source<IAsyncConsumator<T1>> left, IFlowable<Pipe<IConsumator<T1>, IEnumerator<T2>>> right)
//         => throw new NotImplementedException();
//     
//     // batching: IAsyncConsumator → IAsyncEnumerator
//     // внедрение асинхронной цепочки в асинхронную цепочку (совместимое)
//     // примечание: аналогично Next<T1, T2>(this Source<IAsyncEnumerator<T1>> left, IFlowable<Pipe<IEnumerator<T1>, IAsyncConsumator<T2>>> right)
//     [PublicAPI]
//     public static ConfigurablePipe<IAsyncConsumator<T1>, IAsyncEnumerator<T2>, object> Next<T1, T2>(this Source<IAsyncConsumator<T1>> left, IFlowable<Pipe<IConsumator<T1>, IAsyncEnumerator<T2>>> right)
//         => throw new NotImplementedException();
//     
//     #endregion
// }
