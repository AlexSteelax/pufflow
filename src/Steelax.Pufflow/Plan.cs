using Steelax.Pufflow.Abstractions;
#pragma warning disable CS0414 // Field is assigned but its value is never used
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Steelax.Pufflow;

public static class Plan
{
    #region Трансформация блоков

    // Следующий блок получает интерфейс для вытягивания данных
    internal delegate void ConsumatorChain<T>(FlowSource flow, out IConsumator<T> reader);
    internal delegate void AsyncConsumatorChain<T>(FlowSource flow, out IAsyncConsumator<T> reader);
    
    // Следующий блок должен отдавать интерфейс для проталкивания данных
    internal delegate void ProducatorChain<out T>(FlowSource flow, IProducator<T> writer);
    internal delegate void AsyncProducatorChain<out T>(FlowSource flow, IAsyncProducator<T> writer);

    #endregion
    
    #region Типизированная обертка сокрытия источника
    
    public readonly struct Fuse<TNext>
    {
        // object - это Action (ConsumatorChain, AsyncConsumatorChain, ProducatorChain, AsyncProducatorChain)
        
        internal readonly object Next;
        
        internal readonly FlowSource FlowSource;

        internal Fuse(FlowSource flowSource, object next)
        {
            FlowSource = flowSource;
            Next = next;
        }
    }
    
    #endregion

    public readonly struct Pipe<TLet, TRight>
    {
        private readonly Func<object, object> _lazy;
        
        internal Pipe(Func<object, object> lazy) => _lazy = lazy;
        
        internal object Invoke(object prev) => _lazy.Invoke(prev);
    }
    
    public readonly struct Source<T>
    {
        private readonly Func<object> _lazy;
        
        internal Source(Func<object> lazy) => _lazy = lazy;
        
        internal object Invoke() => _lazy.Invoke();
    }
    
    public readonly struct Sink<T>
    {
        private readonly Action<FlowSource, object> _lazy;
        
        internal Sink(Action<FlowSource, object> lazy) => _lazy = lazy;
        
        internal void Invoke(FlowSource flowSource, object prev) => _lazy.Invoke(flowSource, prev);
    }

    public static Fuse<IConsumator<T2>> Next<T1, T2>(this Fuse<IConsumator<T1>> left, Pipe<IConsumator<T1>, IConsumator<T2>> right)
    {
        return new Fuse<IConsumator<T2>>(left.FlowSource, right.Invoke(left.Next));
    }
    
    public static Fuse<IAsyncProducator<T2>> Next<T1, T2>(this Fuse<IAsyncProducator<T1>> left, Pipe<IAsyncProducator<T1>, IAsyncProducator<T2>> right)
    {
        return new Fuse<IAsyncProducator<T2>>(left.FlowSource, right.Invoke(left.Next));
    }
    
    public static Fuse<IAsyncConsumator<T2>> Next<T1, T2>(this Fuse<IAsyncProducator<T1>> left, Pipe<IAsyncProducator<T1>, IAsyncConsumator<T2>> right)
    {
        return new Fuse<IAsyncConsumator<T2>>(left.FlowSource, right.Invoke(left.Next));
    }
    
    public static Fuse<IAsyncProducator<T2>> Next<T1, T2>(this Fuse<IAsyncConsumator<T1>> left, Pipe<IAsyncConsumator<T1>, IAsyncProducator<T2>> right)
    {
        return new Fuse<IAsyncProducator<T2>>(left.FlowSource, right.Invoke(left.Next));
    }
    
    public static Fuse<IConsumator<T>> On<T>(this FlowSource source, Source<IConsumator<T>> right)
    {
        return new Fuse<IConsumator<T>>(source, right.Invoke());
    }
    
    public static Fuse<IProducator<T>> On<T>(this FlowSource source, Source<IProducator<T>> right)
    {
        return new Fuse<IProducator<T>>(source, right.Invoke());
    }
    
    public static void End<T>(this Fuse<IConsumator<T>> left, Sink<IConsumator<T>> right)
    {
        right.Invoke(left.FlowSource, left.Next);
    }
    
    public static void End<T>(this Fuse<IProducator<T>> left, Sink<IProducator<T>> right)
    {
        right.Invoke(left.FlowSource, left.Next);
    }

    #region Base flow
    
    // Example Consumator
    public sealed partial class MyConsumatorFlowProc<T1, T2>
    {
        // Определяем направление по конструктору
        public MyConsumatorFlowProc(object someArg1, object someArg2, IConsumator<T1> reader, out IConsumator<T2> output, FlowContext context)
        {
            _ = someArg1;
            _ = someArg2;
            _ = reader; // из этого читаем сами в этом блоке
            output = null!; // отдаем, чтобы из этого блока можно было читать
        }

        // Кодогенерируем и добавляем аргументы конструктора, скрывая спецификацию flow
        // Таким образом получаем типизированный делегат отложенного конфигурирования, создания инстанса и связывания
        public static Pipe<IConsumator<T1>, IConsumator<T2>> Configure(object someArg1, object someArg2)
        {
            return new Pipe<IConsumator<T1>, IConsumator<T2>>((Func<object, ConsumatorChain<T2>>)Func);

            ConsumatorChain<T2> Func(object prev)
            {
                return Chain;

                void Chain(FlowSource flow, out IConsumator<T2> output)
                {
                    object instance;

                    // Логика прямая
                    switch (prev)
                    {
                        case ConsumatorChain<T1> chain:
                        {
                            chain.Invoke(flow, out var reader);
                            instance = new MyConsumatorFlowProc<T1, T2>(someArg1, someArg2, reader, out output, flow.Context);
                        }
                            break;
                        case AsyncConsumatorChain<T1> chain:
                        {
                            chain.Invoke(flow, out var reader);
                            instance = new MyConsumatorFlowProc<T1, T2>(someArg1, someArg2, reader, out output, flow.Context);
                        }
                            break;
                        default:
                            throw new NotSupportedException();
                    }

                    flow.RegisterDisposable(instance);
                }
            }
        }
    }

    public sealed partial class MyProducatorFlowProc<T1, T2>
    {
        // Определяем направление по конструктору
        // Отдаем output, чтобы в нас писали, и получаем writer, чтобы писать в него
        public MyProducatorFlowProc(object someArg1, object someArg2, out IAsyncProducator<T1> output, IAsyncProducator<T2> writer, FlowContext context)
        {
            _ = someArg1;
            _ = someArg2;
            _ = writer; // получаем из следующего блока, чтобы писать в него
            output = null!; // отдаем от себя, чтобы писали в этот блок
        }

        // Кодогенерируем и добавляем аргументы конструктора, скрывая спецификацию flow
        // Таким образом получаем типизированный делегат отложенного конфигурирования, создания инстанса и связывания
        public static Pipe<IAsyncProducator<T1>, IAsyncProducator<T2>> Configure(object someArg1, object someArg2)
        {
            return new Pipe<IAsyncProducator<T1>, IAsyncProducator<T2>>((Func<object, AsyncProducatorChain<T2>>)Func);

            AsyncProducatorChain<T2> Func(object prev)
            {
                return Chain;

                void Chain(FlowSource flow, IAsyncProducator<T2> writer)
                {
                    object instance;

                    // Логика обратная
                    switch (prev)
                    {
                        case ProducatorChain<T1> chain:
                        {
                            instance = new MyProducatorFlowProc<T1, T2>(someArg1, someArg2, out var output, writer, flow.Context);
                            chain.Invoke(flow, output);
                        }
                            break;
                        case AsyncProducatorChain<T1> chain:
                        {
                            instance = new MyProducatorFlowProc<T1, T2>(someArg1, someArg2, out var output, writer, flow.Context);
                            chain.Invoke(flow, output);
                        }
                            break;
                        default:
                            throw new NotSupportedException();
                    }

                    flow.RegisterDisposable(instance);
                }
            }
        }
    }
    
    public sealed partial class MyProducatorToConsumatorFlowProc<T1, T2>
    {
        // Определяем направление по конструктору
        // Отдаем output, чтобы в нас писали, и отдаем reader, чтобы из нас читали данные
        // Соединитель двух моделей
        public MyProducatorToConsumatorFlowProc(object someArg1, object someArg2, out IAsyncProducator<T1> output, out IAsyncConsumator<T2> reader, FlowContext context)
        {
            _ = someArg1;
            _ = someArg2;
            reader = null!;
            output = null!;
        }

        // Кодогенерируем и добавляем аргументы конструктора, скрывая спецификацию flow
        // Таким образом получаем типизированный делегат отложенного конфигурирования, создания инстанса и связывания
        public static Pipe<IAsyncProducator<T1>, IAsyncConsumator<T2>> Configure(object someArg1, object someArg2)
        {
            return new Pipe<IAsyncProducator<T1>, IAsyncConsumator<T2>>((Func<object, AsyncConsumatorChain<T2>>)Func);

            AsyncConsumatorChain<T2> Func(object prev)
            {
                return Chain;

                void Chain(FlowSource flow, out IAsyncConsumator<T2> reader)
                {
                    object instance;

                    // Логика смешанная
                    switch (prev)
                    {
                        case AsyncProducatorChain<T1> chain:
                        {
                            instance = new MyProducatorToConsumatorFlowProc<T1, T2>(someArg1, someArg2, out var output, out reader, flow.Context);
                            chain.Invoke(flow, output);
                        }
                            break;
                        default:
                            throw new NotSupportedException();
                    }

                    flow.RegisterDisposable(instance);
                }
            }
        }
    }
    
    public sealed partial class MyConsumatorToProducatorFlowProc<T1, T2>
    {
        // Определяем направление по конструктору
        // Принимаем reader, чтобы тянуть данные; принимаем writer, чтобы писать данные
        // Соединитель двух моделей
        public MyConsumatorToProducatorFlowProc(object someArg1, object someArg2, IAsyncConsumator<T1> reader, IAsyncProducator<T2> writer, FlowContext context)
        {
            _ = someArg1;
            _ = someArg2;
            _ = reader;
            _ = writer;
        }

        // Кодогенерируем и добавляем аргументы конструктора, скрывая спецификацию flow
        // Таким образом получаем типизированный делегат отложенного конфигурирования, создания инстанса и связывания
        public static Pipe<IAsyncConsumator<T1>, IAsyncProducator<T2>> Configure(object someArg1, object someArg2)
        {
            return new Pipe<IAsyncConsumator<T1>, IAsyncProducator<T2>>((Func<object, AsyncProducatorChain<T2>>)Func);

            AsyncProducatorChain<T2> Func(object prev)
            {
                return Chain;

                void Chain(FlowSource flow, IAsyncProducator<T2> writer)
                {
                    object instance;

                    // Логика смешанная
                    switch (prev)
                    {
                        case AsyncConsumatorChain<T1> chain:
                        {
                            chain.Invoke(flow, out var reader);
                            instance = new MyConsumatorToProducatorFlowProc<T1, T2>(someArg1, someArg2, reader, writer, flow.Context);
                        }
                            break;
                        default:
                            throw new NotSupportedException();
                    }

                    flow.RegisterDisposable(instance);
                }
            }
        }
    }
    
    public sealed partial class MyConsumatorSourceFlowProc<T>
    {
        // Определяем направление по конструктору
        public MyConsumatorSourceFlowProc(object someArg1, object someArg2, out IConsumator<T> output, FlowContext context)
        {
            _ = someArg1;
            _ = someArg2;
            output = null!; // отдаем, чтобы из этого блока можно было читать
        }

        // Кодогенерируем и добавляем аргументы конструктора, скрывая спецификацию flow
        // Таким образом получаем типизированный делегат отложенного конфигурирования, создания инстанса и связывания
        public static Source<IConsumator<T>> Configure(object someArg1, object someArg2)
        {
            return new Source<IConsumator<T>>((Func<ConsumatorChain<T>>)Func);

            ConsumatorChain<T> Func()
            {
                return Chain;

                void Chain(FlowSource flow, out IConsumator<T> output)
                {
                    // Логика прямая
                    var instance = new MyConsumatorSourceFlowProc<T>(someArg1, someArg2, out output, flow.Context);

                    flow.RegisterDisposable(instance);
                }
            }
        }
    }
    
    public sealed partial class MyProducatorSourceFlowProc<T>
    {
        // Определяем направление по конструктору
        public MyProducatorSourceFlowProc(object someArg1, object someArg2, IProducator<T> writer, FlowContext context)
        {
            _ = someArg1;
            _ = someArg2;
        }

        // Кодогенерируем и добавляем аргументы конструктора, скрывая спецификацию flow
        // Таким образом получаем типизированный делегат отложенного конфигурирования, создания инстанса и связывания
        public static Source<IProducator<T>> Configure(object someArg1, object someArg2)
        {
            return new Source<IProducator<T>>((Func<ProducatorChain<T>>)Func);

            ProducatorChain<T> Func()
            {
                return Chain;

                void Chain(FlowSource flow, IProducator<T> writer)
                {
                    // Логика прямая
                    var instance = new MyProducatorSourceFlowProc<T>(someArg1, someArg2, writer, flow.Context);

                    flow.RegisterDisposable(instance);
                }
            }
        }
    }
    
    public sealed partial class MyConsumatorSinkFlowProc<T>
    {
        // Определяем направление по конструктору
        public MyConsumatorSinkFlowProc(object someArg1, object someArg2, IConsumator<T> writer, FlowContext context)
        {
            _ = someArg1;
            _ = someArg2;
        }

        // Кодогенерируем и добавляем аргументы конструктора, скрывая спецификацию flow
        // Таким образом получаем типизированный делегат отложенного конфигурирования, создания инстанса и связывания
        public static Sink<IConsumator<T>> Configure(object someArg1, object someArg2)
        {
            return new Sink<IConsumator<T>>((Action<FlowSource, object>)Func);

            void Func(FlowSource flow, object prev)
            {
                object instance;

                // Логика прямая
                switch (prev)
                {
                    case ConsumatorChain<T> chain:
                    {
                        chain.Invoke(flow, out var writer);
                        instance = new MyConsumatorSinkFlowProc<T>(someArg1, someArg2, writer, flow.Context);
                    }
                        break;
                    default:
                        throw new NotSupportedException();
                }

                flow.RegisterDisposable(instance);
            }
        }
    }
    
    public sealed partial class MyProducatorSinkFlowProc<T>
    {
        // Определяем направление по конструктору
        public MyProducatorSinkFlowProc(object someArg1, object someArg2, out IProducator<T> writer, FlowContext context)
        {
            _ = someArg1;
            _ = someArg2;
            writer = null!;
        }

        // Кодогенерируем и добавляем аргументы конструктора, скрывая спецификацию flow
        // Таким образом получаем типизированный делегат отложенного конфигурирования, создания инстанса и связывания
        public static Sink<IProducator<T>> Configure(object someArg1, object someArg2)
        {
            return new Sink<IProducator<T>>((Action<FlowSource, object>)Func);

            void Func(FlowSource flow, object prev)
            {
                object instance;

                // Логика прямая
                switch (prev)
                {
                    case ProducatorChain<T> chain:
                    {
                        instance = new MyProducatorSinkFlowProc<T>(someArg1, someArg2, out var writer, flow.Context);
                        chain.Invoke(flow, writer);
                    }
                        break;
                    default:
                        throw new NotSupportedException();
                }

                flow.RegisterDisposable(instance);
            }
        }
    }
    
    #endregion
    
    #region Fallback flow
    
    
    
    #endregion
    
    
    class Test
    {
        void Test1()
        {
            var startNode = new Fuse<IConsumator<int>>();
            var next = startNode.Next(MyConsumatorFlowProc<int, long>.Configure(null!, null!));
        }
        
        void Test2()
        {
            var startNode = new Fuse<IAsyncProducator<int>>();
            var next = startNode.Next(MyProducatorFlowProc<int, long>.Configure(null!, null!));
        }
        
        void Test3()
        {
            var startNode = new Fuse<IAsyncProducator<int>>();
            var next = startNode.Next(MyProducatorToConsumatorFlowProc<int, long>.Configure(null!, null!));
        }

        void Test4()
        {
            var startNode = new Fuse<IAsyncConsumator<int>>();
            var next = startNode.Next(MyConsumatorToProducatorFlowProc<int, long>.Configure(null!, null!));
        }

        void Test5()
        {
            var flowSource = new FlowSource();
            var startNode = flowSource.On(MyConsumatorSourceFlowProc<int>.Configure(null!, null!));
        }
        
        void Test6()
        {
            var flowSource = new FlowSource();
            var startNode = flowSource.On(MyProducatorSourceFlowProc<int>.Configure(null!, null!));
        }

        void Test7()
        {
            var startNode = new Fuse<IConsumator<int>>();
            startNode.End(MyConsumatorSinkFlowProc<int>.Configure(null!, null!));
        }
        
        void Test8()
        {
            var startNode = new Fuse<IProducator<int>>();
            startNode.End(MyProducatorSinkFlowProc<int>.Configure(null!, null!));
        }
    }
}