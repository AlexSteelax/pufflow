using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow;

public static partial class FlowExt
{
    /// <summary>
    ///     Chains a synchronous consumator source to a synchronous consumator pipe.
    /// </summary>
    [PublicAPI]
    public static Source<IConsumator<T2>> Next<T1, T2>(this Source<IConsumator<T1>> left, IFlowable<Pipe<IConsumator<T1>, IConsumator<T2>>> rightFlow)
    {
        throw new NotImplementedException();
    }

    /// <summary>
    ///     Chains an async consumator source to an async consumator pipe (async→async).
    /// </summary>
    [PublicAPI]
    public static Source<IAsyncConsumator<T2>> Next<T1, T2>(this Source<IAsyncConsumator<T1>> left, IFlowable<Pipe<IAsyncConsumator<T1>, IAsyncConsumator<T2>>> rightFlow)
    {
        var rightMeta = FlowMetaNode.Create(rightFlow, FlowKind.AsyncConsumator, FlowKind.AsyncConsumator);
        var merged = FlowMetaNode.Merge(left.Meta, rightMeta, left.Context);
        return new Source<IAsyncConsumator<T2>>(merged, left.Context);
    }

    /// <summary>
    ///     Chains an async consumator pipe to an async consumator pipe (async→async).
    /// </summary>
    [PublicAPI]
    public static Source<IAsyncConsumator<T2>> Next<T1, TMid, T2>(this Pipe<IAsyncConsumator<T1>, IAsyncConsumator<TMid>> left, IFlowable<Pipe<IAsyncConsumator<TMid>, IAsyncConsumator<T2>>> rightFlow)
    {
        var rightMeta = FlowMetaNode.Create(rightFlow, FlowKind.AsyncConsumator, FlowKind.AsyncConsumator);
        var merged = FlowMetaNode.Merge(left.Meta, rightMeta, left.Context);
        return new Source<IAsyncConsumator<T2>>(merged, left.Context);
    }

    /// <summary>
    ///     Chains a synchronous consumator source to an async consumator pipe (sync→async transition).
    /// </summary>
    [PublicAPI]
    public static Source<IAsyncConsumator<T2>> Next<T1, T2>(this Source<IConsumator<T1>> left, IFlowable<Pipe<IConsumator<T1>, IAsyncConsumator<T2>>> rightFlow)
    {
        throw new NotImplementedException();
    }

    /// <summary>
    ///     Chains an enumerator source to a synchronous consumator pipe (enumerator→consumator bridge).
    /// </summary>
    [PublicAPI]
    public static Source<IConsumator<T2>> Next<T1, T2>(this Source<IEnumerator<T1>> left, IFlowable<Pipe<IEnumerator<T1>, IConsumator<T2>>> rightFlow)
    {
        throw new NotImplementedException();
    }

    /// <summary>
    ///     Chains an enumerator source to an async consumator pipe (enumerator→async consumator bridge).
    /// </summary>
    [PublicAPI]
    public static Source<IAsyncConsumator<T2>> Next<T1, T2>(this Source<IEnumerator<T1>> left, IFlowable<Pipe<IEnumerator<T1>, IAsyncConsumator<T2>>> rightFlow)
    {
        throw new NotImplementedException();
    }

    /// <summary>
    ///     Chains an async enumerator source to an async consumator pipe (async enumerator→async consumator bridge).
    /// </summary>
    [PublicAPI]
    public static Source<IAsyncConsumator<T2>> Next<T1, T2>(this Source<IAsyncEnumerator<T1>> left, IFlowable<Pipe<IAsyncEnumerator<T1>, IAsyncConsumator<T2>>> rightFlow)
    {
        var rightMeta = FlowMetaNode.Create(rightFlow, FlowKind.AsyncEnumerator, FlowKind.AsyncConsumator);
        var merged = FlowMetaNode.Merge(left.Meta, rightMeta, left.Context);
        return new Source<IAsyncConsumator<T2>>(merged, left.Context);
    }
    
    /// <summary>
    ///     Chains an async consumator source to an async producator pipe (async pull → async push). The
    ///     source node and the pipe are grouped into a <see cref="FlowMetaCollection" /> for reverse-order
    ///     resolution: the downstream producator target is created by the terminal sink and fed upstream
    ///     through the pipe into the consumator source.
    /// </summary>
    [PublicAPI]
    public static Source<IAsyncProducator<T2>> Next<T1, T2>(this Source<IAsyncConsumator<T1>> left, IFlowable<Pipe<IAsyncConsumator<T1>, IAsyncProducator<T2>>> rightFlow)
    {
        var collection = GetOrCreateCollection(left.Meta);
        collection.Push(FlowMetaNode.Create(rightFlow, FlowKind.AsyncConsumator, FlowKind.AsyncProducator));
        return new Source<IAsyncProducator<T2>>(collection, left.Context);
    }
    
    /// <summary>
    ///     Chains an async producator source to a composite push→pull pipe (async push → async pull). The
    ///     composite (<c>Fuse(out IAsyncProducator, out IAsyncConsumator, ctx)</c>, e.g. a passive buffer
    ///     bridge) exposes a push input producer the upstream source writes into and a pull output stream
    ///     the downstream consumator reads. The source and the composite are grouped into a
    ///     <see cref="FlowMetaCollection" /> for reverse-order resolution.
    /// </summary>
    [PublicAPI]
    public static Source<IAsyncConsumator<T2>> Next<T1, T2>(this Source<IAsyncProducator<T1>> left, IFlowable<Pipe<IAsyncProducator<T1>, IAsyncConsumator<T2>>> rightFlow)
    {
        var collection = GetOrCreateCollection(left.Meta);
        collection.Push(FlowMetaNode.Create(rightFlow, FlowKind.OutAsyncProducator, FlowKind.OutAsyncConsumator));
        return new Source<IAsyncConsumator<T2>>(collection, left.Context);
    }
}
