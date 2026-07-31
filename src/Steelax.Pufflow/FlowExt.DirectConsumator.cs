using Steelax.Pufflow.Common;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow;

public static partial class FlowExt
{
    // collapsable
    // синхронная цепочка
    [PublicAPI]
    public static Source<IConsumator<T2>> Next<T1, T2>(this Source<IConsumator<T1>> left, IFlowable<Pipe<IConsumator<T1>, IConsumator<T2>>> rightFlow)
    {
        // var leftMethod = FlowMarshal.GetConsumator(left);
        // var right = rightFlow.GetFlow();
        // var rightMethod = FlowMarshal.GetPipeMethod(right.Instance, "GetConsumator");
        //
        // var leftResult = leftMethod.Invoke(left.Instance, null)!;
        // var rightResult = rightMethod.Invoke(right.Instance, [leftResult, CancellationToken.None])!;
        // var rightType = rightResult.GetType();
        //
        // var mergedType = typeof(InternalConsumator<,>).MakeGenericType(typeof(T2), rightType);
        // var merged = Activator.CreateInstance(mergedType, rightResult)!;
        //
        // return new Source<IConsumator<T2>>(merged);
        throw new NotImplementedException();
    }
    
    // collapsable
    // асинхронная цепочка
    [PublicAPI]
    public static Source<IAsyncConsumator<T2>> Next<T1, T2>(this Source<IAsyncConsumator<T1>> left, IFlowable<Pipe<IAsyncConsumator<T1>, IAsyncConsumator<T2>>> rightFlow)
    {
        // var leftMethod = FlowMarshal.GetAsyncConsumator(left);
        // var right = rightFlow.GetFlow();
        // var rightMethod = FlowMarshal.GetPipeMethod(right.Instance, "GetAsyncConsumator");
        //
        // var leftResult = leftMethod.Invoke(left.Instance, [CancellationToken.None])!;
        // var rightResult = rightMethod.Invoke(right.Instance, [leftResult, CancellationToken.None])!;
        // var rightType = rightResult.GetType();
        //
        // var mergedType = typeof(InternalAsyncConsumator<,>).MakeGenericType(typeof(T2), rightType);
        // var merged = Activator.CreateInstance(mergedType, rightResult)!;
        //
        // return new Source<IAsyncConsumator<T2>>(merged);
        throw new NotImplementedException();
    }
    
    // collapsable
    // смена модели на асинхронную
    [PublicAPI]
    public static Source<IAsyncConsumator<T2>> Next<T1, T2>(this Source<IConsumator<T1>> left, IFlowable<Pipe<IConsumator<T1>, IAsyncConsumator<T2>>> rightFlow)
    {
        // var leftMethod = FlowMarshal.GetConsumator(left);
        // var right = rightFlow.GetFlow();
        // var rightMethod = FlowMarshal.GetPipeMethod(right.Instance, "GetAsyncConsumator");
        //
        // var leftResult = leftMethod.Invoke(left.Instance, null)!;
        // var rightResult = rightMethod.Invoke(right.Instance, [leftResult, CancellationToken.None])!;
        // var rightType = rightResult.GetType();
        //
        // var mergedType = typeof(InternalAsyncConsumator<,>).MakeGenericType(typeof(T2), rightType);
        // var merged = Activator.CreateInstance(mergedType, rightResult)!;
        //
        // return new Source<IAsyncConsumator<T2>>(merged);
        throw new NotImplementedException();
    }
    
    // collapsable
    // асинхронная цепочка (совместимая)
    // Enumerator → Consumator
    [PublicAPI]
    public static Source<IConsumator<T2>> Next<T1, T2>(this Source<IEnumerator<T1>> left, IFlowable<Pipe<IEnumerator<T1>, IConsumator<T2>>> rightFlow)
    {
        // var leftMethod = FlowMarshal.GetEnumerator(left);
        // var right = rightFlow.GetFlow();
        // var rightMethod = FlowMarshal.GetPipeMethod(right.Instance, "GetConsumator");
        //
        // var leftResult = leftMethod.Invoke(left.Instance, null)!;
        // var rightResult = rightMethod.Invoke(right.Instance, [leftResult, CancellationToken.None])!;
        // var rightType = rightResult.GetType();
        //
        // var mergedType = typeof(InternalConsumator<,>).MakeGenericType(typeof(T2), rightType);
        // var merged = Activator.CreateInstance(mergedType, rightResult)!;
        //
        // return new Source<IConsumator<T2>>(merged);
        throw new NotImplementedException();
    }
    
    // collapsable
    // переход к асинхронной модели (совместимый)
    // Enumerator → AsyncConsumator
    [PublicAPI]
    public static Source<IAsyncConsumator<T2>> Next<T1, T2>(this Source<IEnumerator<T1>> left, IFlowable<Pipe<IEnumerator<T1>, IAsyncConsumator<T2>>> rightFlow)
    {
        // var leftMethod = FlowMarshal.GetEnumerator(left);
        // var right = rightFlow.GetFlow();
        // var rightMethod = FlowMarshal.GetPipeMethod(right.Instance, "GetAsyncConsumator");
        //
        // var leftResult = leftMethod.Invoke(left.Instance, null)!;
        // var rightResult = rightMethod.Invoke(right.Instance, [leftResult, CancellationToken.None])!;
        // var rightType = rightResult.GetType();
        //
        // var mergedType = typeof(InternalAsyncConsumator<,>).MakeGenericType(typeof(T2), rightType);
        // var merged = Activator.CreateInstance(mergedType, rightResult)!;
        //
        // return new Source<IAsyncConsumator<T2>>(merged);
        throw new NotImplementedException();
    }
    
    // collapsable
    // асинхронная цепочка (совместимая)
    // AsyncEnumerator → AsyncConsumator
    [PublicAPI]
    public static Source<IAsyncConsumator<T2>> Next<T1, T2>(this Source<IAsyncEnumerator<T1>> left, IFlowable<Pipe<IAsyncEnumerator<T1>, IAsyncConsumator<T2>>> rightFlow)
    {
        // var leftMethod = FlowMarshal.GetAsyncEnumerator(left);
        // var right = rightFlow.GetFlow();
        // var rightMethod = FlowMarshal.GetPipeMethod(right.Instance, "GetAsyncConsumator");
        //
        // var leftResult = leftMethod.Invoke(left.Instance, [CancellationToken.None])!;
        // var rightResult = rightMethod.Invoke(right.Instance, [leftResult, CancellationToken.None])!;
        // var rightType = rightResult.GetType();
        //
        // var mergedType = typeof(InternalAsyncConsumator<,>).MakeGenericType(typeof(T2), rightType);
        // var merged = Activator.CreateInstance(mergedType, rightResult)!;
        //
        // return new Source<IAsyncConsumator<T2>>(merged);
        throw new NotImplementedException();
    }
}
