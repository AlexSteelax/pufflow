using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow;

public static partial class FlowExt
{
    /// <summary>
    ///     Connects an upstream source/pipe node to a terminal sink, forming a complete pipeline.
    /// </summary>
    /// <typeparam name="T">The element type flowing through the pipeline.</typeparam>
    /// <param name="left">The upstream node (a source or a previously connected pipe).</param>
    /// <param name="right">The terminal sink component.</param>
    /// <returns>A sink marker representing the terminal stage.</returns>
    [PublicAPI]
    public static Sink<IAsyncEnumerator<T>> End<T>(this Source<IAsyncEnumerator<T>> left,
        IFlowable<Sink<IAsyncEnumerator<T>>> right)
    {
        var rightMeta = FlowMetaNode.Create(right, FlowKind.AsyncEnumerator, FlowKind.None);
        var merged = FlowMetaNode.Merge(left.Meta, rightMeta, left.Context);
        return new Sink<IAsyncEnumerator<T>>(merged, left.Context);
    }

    /// <summary>
    ///     Connects an upstream pipe node to a terminal sink, forming a complete pipeline.
    /// </summary>
    /// <typeparam name="T">The element type flowing through the pipeline.</typeparam>
    /// <param name="left">The upstream pipe node.</param>
    /// <param name="right">The terminal sink component.</param>
    /// <returns>A sink marker representing the terminal stage.</returns>
    [PublicAPI]
    public static Sink<IAsyncEnumerator<T>> End<T>(this Pipe<IAsyncEnumerator<T>, IAsyncEnumerator<T>> left, IFlowable<Sink<IAsyncEnumerator<T>>> right)
    {
        var rightMeta = FlowMetaNode.Create(right, FlowKind.AsyncEnumerator, FlowKind.None);
        var merged = FlowMetaNode.Merge(left.Meta, rightMeta, left.Context);
        return new Sink<IAsyncEnumerator<T>>(merged, left.Context);
    }

    /// <summary>
    ///     Connects an upstream async consumator source/pipe node to a terminal sink, forming a complete pipeline.
    /// </summary>
    /// <typeparam name="T">The element type flowing through the pipeline.</typeparam>
    /// <param name="left">The upstream node (a source or a previously connected pipe).</param>
    /// <param name="right">The terminal sink component.</param>
    /// <returns>A sink marker representing the terminal stage.</returns>
    [PublicAPI]
    public static Sink<IAsyncConsumator<T>> End<T>(this Source<IAsyncConsumator<T>> left, IFlowable<Sink<IAsyncConsumator<T>>> right)
    {
        var rightMeta = FlowMetaNode.Create(right, FlowKind.AsyncConsumator, FlowKind.None);

        // A collection groups a push source and a composite push→pull pipe (e.g. a passive buffer bridge)
        // for reverse-order resolution: the composite creates both the push input and the pull output, the
        // push source writes into the input, the sink consumes the output. Build resolves the chain.
        var merged = FlowMetaNode.Merge(left.Meta, rightMeta, left.Context);
        if (merged is FlowMetaCollection collection)
            collection.Build(left.Context);

        return new Sink<IAsyncConsumator<T>>(merged, left.Context);
    }

    /// <summary>
    ///     Connects an upstream synchronous consumator source/pipe node to a terminal sink, forming a
    ///     complete pipeline.
    /// </summary>
    /// <typeparam name="T">The element type flowing through the pipeline.</typeparam>
    /// <param name="left">The upstream node (a source or a previously connected pipe).</param>
    /// <param name="right">The terminal sink component.</param>
    /// <returns>A sink marker representing the terminal stage.</returns>
    [PublicAPI]
    public static Sink<IConsumator<T>> End<T>(this Source<IConsumator<T>> left, IFlowable<Sink<IConsumator<T>>> right)
    {
        var rightMeta = FlowMetaNode.Create(right, FlowKind.Consumator, FlowKind.None);

        var merged = FlowMetaNode.Merge(left.Meta, rightMeta, left.Context);
        if (merged is FlowMetaCollection collection)
            collection.Build(left.Context);

        return new Sink<IConsumator<T>>(merged, left.Context);
    }

    /// <summary>
    ///     Connects an upstream async consumator pipe node to a terminal sink, forming a complete pipeline.
    /// </summary>
    /// <typeparam name="T">The element type flowing through the pipeline.</typeparam>
    /// <param name="left">The upstream pipe node.</param>
    /// <param name="right">The terminal sink component.</param>
    /// <returns>A sink marker representing the terminal stage.</returns>
    [PublicAPI]
    public static Sink<IAsyncConsumator<T>> End<T>(this Pipe<IAsyncConsumator<T>, IAsyncConsumator<T>> left, IFlowable<Sink<IAsyncConsumator<T>>> right)
    {
        var rightMeta = FlowMetaNode.Create(right, FlowKind.AsyncConsumator, FlowKind.None);
        var merged = FlowMetaNode.Merge(left.Meta, rightMeta, left.Context);
        return new Sink<IAsyncConsumator<T>>(merged, left.Context);
    }
}
