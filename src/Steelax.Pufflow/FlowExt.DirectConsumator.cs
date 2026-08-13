using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow;

public static partial class FlowExt
{
    /// <summary>
    ///     Chains a synchronous consumator source to a synchronous consumator pipe.
    /// </summary>
    [PublicAPI]
    public static Source<IConsumator<T2>> Next<T1, T2>(this Source<IConsumator<T1>> left,
        IFlowable<Pipe<IConsumator<T1>, IConsumator<T2>>> rightFlow)
    {
        throw new NotImplementedException();
    }

    /// <summary>
    ///     Chains an async consumator source to an async consumator pipe.
    /// </summary>
    [PublicAPI]
    public static Source<IAsyncConsumator<T2>> Next<T1, T2>(this Source<IAsyncConsumator<T1>> left,
        IFlowable<Pipe<IAsyncConsumator<T1>, IAsyncConsumator<T2>>> rightFlow)
    {
        throw new NotImplementedException();
    }

    /// <summary>
    ///     Chains a synchronous consumator source to an async consumator pipe (sync→async transition).
    /// </summary>
    [PublicAPI]
    public static Source<IAsyncConsumator<T2>> Next<T1, T2>(this Source<IConsumator<T1>> left,
        IFlowable<Pipe<IConsumator<T1>, IAsyncConsumator<T2>>> rightFlow)
    {
        throw new NotImplementedException();
    }

    /// <summary>
    ///     Chains an enumerator source to a synchronous consumator pipe (enumerator→consumator bridge).
    /// </summary>
    [PublicAPI]
    public static Source<IConsumator<T2>> Next<T1, T2>(this Source<IEnumerator<T1>> left,
        IFlowable<Pipe<IEnumerator<T1>, IConsumator<T2>>> rightFlow)
    {
        throw new NotImplementedException();
    }

    /// <summary>
    ///     Chains an enumerator source to an async consumator pipe (enumerator→async consumator bridge).
    /// </summary>
    [PublicAPI]
    public static Source<IAsyncConsumator<T2>> Next<T1, T2>(this Source<IEnumerator<T1>> left,
        IFlowable<Pipe<IEnumerator<T1>, IAsyncConsumator<T2>>> rightFlow)
    {
        throw new NotImplementedException();
    }

    /// <summary>
    ///     Chains an async enumerator source to an async consumator pipe (async enumerator→async consumator bridge).
    /// </summary>
    [PublicAPI]
    public static Source<IAsyncConsumator<T2>> Next<T1, T2>(this Source<IAsyncEnumerator<T1>> left,
        IFlowable<Pipe<IAsyncEnumerator<T1>, IAsyncConsumator<T2>>> rightFlow)
    {
        throw new NotImplementedException();
    }
}