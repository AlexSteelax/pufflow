using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow;

public static partial class FlowExt
{
    // collapsable
    // синхронная цепочка
    [PublicAPI]
    public static Source<IEnumerator<T2>> Next<T1, T2>(this Source<IEnumerator<T1>> left, IFlowable<Pipe<IEnumerator<T1>, IEnumerator<T2>>> rightFlow)
    {
        // var leftEnumeratorMethod = FlowMarshal.GetEnumerator(left);
        // var right = rightFlow.GetFlow();
        // var rightEnumeratorMethod = FlowMarshal.GetPipeMethod(right.Instance, "GetEnumerator");
        //
        // var leftEnumerator = leftEnumeratorMethod.Invoke(left.Instance, null)!;
        // var rightEnumerator = rightEnumeratorMethod.Invoke(right.Instance, [leftEnumerator])!;
        // var rightEnumeratorType = rightEnumerator.GetType();
        //
        // var mergedType = typeof(InternalEnumerator<,>).MakeGenericType(typeof(T2), rightEnumeratorType);
        // var mergedInstance = Activator.CreateInstance(mergedType, rightEnumerator)!;
        //
        // return new Source<IEnumerator<T2>>(mergedInstance);
        throw new NotImplementedException();
    }
    
    // collapsable
    // асинхронная цепочка
    [PublicAPI]
    public static Source<IAsyncEnumerator<T2>> Next<T1, T2>(this Source<IAsyncEnumerator<T1>> left, IFlowable<Pipe<IAsyncEnumerator<T1>, IAsyncEnumerator<T2>>> right)
    {
        var leftEnumerator = FlowMarshal.GetAsyncEnumerator(left.Instance, left.Context);
        Debug.Assert(leftEnumerator is not null);
        
        var rightEnumerator = FlowMarshal.GetAsyncEnumerator(right, left.Context, leftEnumerator);
        Debug.Assert(rightEnumerator is not null);

        return new Source<IAsyncEnumerator<T2>>(rightEnumerator, left.Context);
    }
    
    // collapsable
    // переход к асинхронной модели
    [PublicAPI]
    public static Source<IAsyncEnumerator<T2>> Next<T1, T2>(this Source<IEnumerator<T1>> left, IFlowable<Pipe<IEnumerator<T1>, IAsyncEnumerator<T2>>> rightFlow)
    {
        // var leftMethod = FlowMarshal.GetEnumerator(left);
        // var right = rightFlow.GetFlow();
        // var rightMethod = FlowMarshal.GetPipeMethod(right.Instance, "GetAsyncEnumerator");
        //
        // var leftResult = leftMethod.Invoke(left.Instance, null)!;
        // var rightResult = rightMethod.Invoke(right.Instance, [leftResult, CancellationToken.None])!;
        // var rightType = rightResult.GetType();
        //
        // var mergedType = typeof(InternalAsyncEnumerator<,>).MakeGenericType(typeof(T2), rightType);
        // var merged = Activator.CreateInstance(mergedType, rightResult)!;
        //
        // return new Source<IAsyncEnumerator<T2>>(merged);
        throw new NotImplementedException();
    }
    
    
    // collapsable
    // синхронная цепочка (совместимая)
    [PublicAPI]
    public static Source<IEnumerator<T2>> Next<T1, T2>(this Source<IConsumator<T1>> left, IFlowable<Pipe<IConsumator<T1>, IEnumerator<T2>>> rightFlow)
    {
        // var leftMethod = FlowMarshal.GetConsumator(left);
        // var right = rightFlow.GetFlow();
        // var rightMethod = FlowMarshal.GetPipeMethod(right.Instance, "GetEnumerator");
        //
        // var leftResult = leftMethod.Invoke(left.Instance, null)!;
        // var rightResult = rightMethod.Invoke(right.Instance, [leftResult, CancellationToken.None])!;
        // var rightType = rightResult.GetType();
        //
        // var mergedType = typeof(InternalEnumerator<,>).MakeGenericType(typeof(T2), rightType);
        // var merged = Activator.CreateInstance(mergedType, rightResult)!;
        //
        // return new Source<IEnumerator<T2>>(merged);
        throw new NotImplementedException();
    }
    
    // collapsable
    // смена модели на асинхронную (совместимая)
    [PublicAPI]
    public static Source<IAsyncEnumerator<T2>> Next<T1, T2>(this Source<IConsumator<T1>> left, IFlowable<Pipe<IConsumator<T1>, IAsyncEnumerator<T2>>> rightFlow)
    {
        // var leftMethod = FlowMarshal.GetConsumator(left);
        // var right = rightFlow.GetFlow();
        // var rightMethod = FlowMarshal.GetPipeMethod(right.Instance, "GetAsyncEnumerator");
        //
        // var leftResult = leftMethod.Invoke(left.Instance, null)!;
        // var rightResult = rightMethod.Invoke(right.Instance, [leftResult, CancellationToken.None])!;
        // var rightType = rightResult.GetType();
        //
        // var mergedType = typeof(InternalAsyncEnumerator<,>).MakeGenericType(typeof(T2), rightType);
        // var merged = Activator.CreateInstance(mergedType, rightResult)!;
        //
        // return new Source<IAsyncEnumerator<T2>>(merged);
        throw new NotImplementedException();
    }
    
    // collapsable
    // асинхронная цепочка (совместимая)
    [PublicAPI]
    public static Source<IAsyncEnumerator<T2>> Next<T1, T2>(this Source<IAsyncConsumator<T1>> left, IFlowable<Pipe<IAsyncConsumator<T1>, IAsyncEnumerator<T2>>> rightFlow)
    {
        // var leftMethod = FlowMarshal.GetAsyncConsumator(left);
        // var right = rightFlow.GetFlow();
        // var rightMethod = FlowMarshal.GetPipeMethod(right.Instance, "GetAsyncEnumerator");
        //
        // var leftResult = leftMethod.Invoke(left.Instance, [CancellationToken.None])!;
        // var rightResult = rightMethod.Invoke(right.Instance, [leftResult, CancellationToken.None])!;
        // var rightType = rightResult.GetType();
        //
        // var mergedType = typeof(InternalAsyncEnumerator<,>).MakeGenericType(typeof(T2), rightType);
        // var merged = Activator.CreateInstance(mergedType, rightResult)!;
        //
        // return new Source<IAsyncEnumerator<T2>>(merged);
        throw new NotImplementedException();
    }
}
