using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow;

public static partial class FlowExt
{
    #region Producator

    // collapsable
    [PublicAPI]
    public static Source<IProducator<T2>> Next<T1, T2>(this Source<IProducator<T1>> left, IFlowable<Pipe<IProducator<T1>, IProducator<T2>>> right)
        => throw new NotImplementedException();

    // collapsable
    [PublicAPI]
    public static Source<IAsyncProducator<T2>> Next<T1, T2>(this Source<IAsyncProducator<T1>> left, IFlowable<Pipe<IAsyncProducator<T1>, IAsyncProducator<T2>>> right)
        => throw new NotImplementedException();

    #endregion
}