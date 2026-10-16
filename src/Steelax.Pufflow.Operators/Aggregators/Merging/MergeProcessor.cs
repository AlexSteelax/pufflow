// using System.Runtime.CompilerServices;
// using Steelax.Pufflow.Operators.Common;
// using Steelax.Pufflow.Operators.Internal;
// using Steelax.Toolkit.HighPerformance.Concurrency.Primitives;
// using Steelax.Toolkit.HighPerformance.Primitives;
//
// namespace Steelax.Pufflow.Operators.Aggregators.Merging;
//
// [Flow]
// internal sealed partial class MergeProcessor<T> : IAsyncConsumator<Carrier<T>>, IAsyncDisposable
// {
//     private readonly FanInSlim _fanIn;
//     private readonly Consumator[] _sources;
//     private readonly WatermarkState[] _watermarks;
//
//     private CancellationTokenRegistration _cancellationTokenRegistration;
//     private CancellationToken _cancellationToken;
//     
//     private int _idx = -1;
//     private int _watching;
//     private bool _init;
//
//     private SlotSet _slotSet;
//     
//     public MergeProcessor(IAsyncConsumator<Carrier<T>>[] sources)
//     {
//         // Поддерживаем только до 32 источников для слияния. Этого должно быть более чем достаточно.
//         ArgumentOutOfRangeException.ThrowIfGreaterThan(sources.Length, 32, nameof(sources));
//         
//         _fanIn = new FanInSlim();
//         _sources = new Consumator[sources.Length];
//         _watermarks = new WatermarkState[sources.Length];
//         
//         // Счетчик наблюдаемых источников
//         _watching = sources.Length;
//
//         for (var i = 0; i < sources.Length; i++)
//         {
//             var consumator = ManualConsumator<Carrier<T>>.Create(sources[i]);
//             
//             consumator.OnReady += _fanIn.GetSignalCallback(i).Handler;
//             
//             _sources[i] = new Consumator(consumator);
//         }
//     }
//     
//     public void Fuse(out IAsyncConsumator<Carrier<T>> target, FlowContext context)
//     {
//         // Регистрируем терминальное завершение при соответствующем сигнале
//         _cancellationTokenRegistration = context.Token.UnsafeRegister(static state => Unsafe.As<object, FanInSlim>(ref state!).Complete(), _fanIn);
//         _cancellationToken = context.Token;
//         target = this;
//     }
//
//     private bool TryGetWaiting(out Carrier<T> value)
//     {
//         while (_slotSet.TryPop(out var index, out _slotSet))
//         {
//             ref var source = ref _sources[index];
//             
//             
//         }
//
//         value = default;
//         return false;
//     }
//
//     public bool TryRead(out Carrier<T> value)
//     {
//         while (!_cancellationToken.IsCancellationRequested)
//         {
//             // Сразу вычисляем следующий индексатор (начинаем с -1 по умолчанию значения)
//             // Также завершение полного цикла устанавливает флаг инициализации (необходимо для запуска цикла опроса)
//             if (++_idx > _sources.Length)
//             {
//                 _idx = 0;
//                 _init = true;
//             }
//             
//             ref var source = ref _sources[_idx];
//
//             var consumator = source.Instance;
//
//             // Быстрый пропуск завершенного источника
//             if (source.Completed)
//                 continue;
//             
//             // Проверяем необходимо ли источник учесть как завершенный
//             if (consumator.IsCompleted)
//             {
//                 if (!source.Completed)
//                     _watching -= 1;
//                 
//                 source.Completed = true;
//                 continue;
//             }
//
//             if (consumator.TryGet(out var item))
//             {
//                 value = item;
//
//                 _watermarks[_idx].Watermark = item.Watermark;
//                 
//                 consumator.Ack();
//                 return true;
//             }
//         }
//         
//         value = default;
//         return false;
//     }
//
//     public bool IsCompleted => _watching == 0;
//     
//     public ValueTask<bool> WaitToReadAsync() => _fanIn.WaitAsync();
//
//     public async ValueTask DisposeAsync()
//     {
//         await _cancellationTokenRegistration.DisposeAsync();
//     }
//
//     private struct Consumator(ManualConsumator<Carrier<T>> instance)
//     {
//         [PublicAPI]
//         public readonly ManualConsumator<Carrier<T>> Instance = instance;
//         
//         [PublicAPI]
//         public State State;
//     }
//
//     private enum State
//     {
//         None,
//         Waiting,
//         Completed
//     }
//     
//     private struct WatermarkState()
//     {
//         private const long NotInitWatermark = -2;
//         
//         private Watermark _watermark = Watermark.From(NotInitWatermark);
//         
//         [PublicAPI]
//         public Watermark Watermark
//         {
//             get => _watermark;
//             set
//             {
//                 if (value > _watermark)
//                     _watermark = value;
//             }
//         }
//
//         [PublicAPI]
//         public bool IsSet => _watermark != NotInitWatermark;
//
//         [PublicAPI]
//         public void Reset() => _watermark = Watermark.From(NotInitWatermark);
//     }
// }
//
