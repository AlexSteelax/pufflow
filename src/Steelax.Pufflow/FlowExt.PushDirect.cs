using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow;

public static partial class FlowExt
{
    /// <summary>
    ///     Chains an async producator source to an async producator pipe (async push→async push). The
    ///     source node and the pipe are grouped into a <see cref="FlowMetaCollection" /> for reverse-order
    ///     resolution.
    /// </summary>
    [PublicAPI]
    public static Source<IAsyncProducator<T2>> Next<T1, T2>(this Source<IAsyncProducator<T1>> left,
        IFlowable<Pipe<IAsyncProducator<T1>, IAsyncProducator<T2>>> right)
    {
        var collection = GetOrCreateCollection(left.Meta);
        collection.Push(FlowMetaNode.Create(right, FlowKind.OutAsyncProducator, FlowKind.AsyncProducator));
        return new Source<IAsyncProducator<T2>>(collection, left.Context);
    }

    /// <summary>
    ///     Chains an async producator pipe to an async producator pipe (async push→async push).
    /// </summary>
    [PublicAPI]
    public static Source<IAsyncProducator<T2>> Next<T1, TMid, T2>(this Pipe<IAsyncProducator<T1>, IAsyncProducator<TMid>> left, IFlowable<Pipe<IAsyncProducator<TMid>, IAsyncProducator<T2>>> right)
    {
        var collection = (FlowMetaCollection)left.Meta;
        collection.Push(FlowMetaNode.Create(right, FlowKind.OutAsyncProducator, FlowKind.AsyncProducator));
        return new Source<IAsyncProducator<T2>>(collection, left.Context);
    }

    /// <summary>
    ///     Connects an async producator source to a terminal async producator sink, resolving the chain.
    /// </summary>
    [PublicAPI]
    public static Sink<IAsyncProducator<T>> End<T>(this Source<IAsyncProducator<T>> left, IFlowable<Sink<IAsyncProducator<T>>> right)
    {
        var collection = GetOrCreateCollection(left.Meta);
        collection.Push(FlowMetaNode.Create(right, FlowKind.AsyncProducator, FlowKind.None));
        collection.Build(left.Context);
        return new Sink<IAsyncProducator<T>>(collection, left.Context);
    }
    
    /// <summary>
    ///     Connects an producator source to a terminal producator sink, resolving the chain.
    /// </summary>
    [PublicAPI]
    public static Sink<IProducator<T>> End<T>(this Source<IProducator<T>> left, IFlowable<Sink<IProducator<T>>> right)
    {
        var collection = GetOrCreateCollection(left.Meta);
        collection.Push(FlowMetaNode.Create(right, FlowKind.Producator, FlowKind.None));
        collection.Build(left.Context);
        return new Sink<IProducator<T>>(collection, left.Context);
    }

    /// <summary>
    ///     Connects an async producator pipe to a terminal async producator sink, resolving the chain.
    /// </summary>
    [PublicAPI]
    public static Sink<IAsyncProducator<T>> End<T>(this Pipe<IAsyncProducator<T>, IAsyncProducator<T>> left, IFlowable<Sink<IAsyncProducator<T>>> right)
    {
        var collection = (FlowMetaCollection)left.Meta;
        collection.Push(FlowMetaNode.Create(right, FlowKind.AsyncProducator, FlowKind.None));
        collection.Build(left.Context);
        return new Sink<IAsyncProducator<T>>(collection, left.Context);
    }

    /// <summary>Returns the existing collection, or creates one seeded with the source node.</summary>
    private static FlowMetaCollection GetOrCreateCollection(FlowMeta meta)
    {
        return meta as FlowMetaCollection ?? new FlowMetaCollection().Push(meta);
    }
}
