namespace Steelax.Pufflow;

/// <summary>
///     A <see cref="FlowMeta" /> that groups nodes for a reverse-order (push) chain, e.g. a producator
///     pipeline. Nodes are pushed in construction order (source first, sink last); <see cref="Merge" />
///     appends the right node and the chain is then resolved back-to-front by <see cref="Build" />, which
///     creates the terminal producer, feeds it upstream through the pipes and finally into the source's
///     <c>Fuse</c>.
/// </summary>
internal sealed class FlowMetaCollection : FlowMeta
{
    private readonly Stack<FlowMetaNode> _stack = new();

    /// <summary>Adds a node to the collection (pushed for later reverse resolution).</summary>
    /// <param name="node">The node to add.</param>
    /// <returns>This collection, for fluent chaining.</returns>
    public FlowMetaCollection Push(FlowMeta node)
    {
        var metaNode = (FlowMetaNode)node;
        Trace.WriteLine($"[FlowMetaCollection] Push: '{metaNode.Kind}' on '{metaNode.Method.Name}'");
        _stack.Push(metaNode);
        return this;
    }

    /// <summary>
    ///     Combines this collection with a right node: the right node is pushed onto the stack and the
    ///     resulting collection is returned (ready for further chaining).
    /// </summary>
    /// <param name="right">The downstream node to append.</param>
    /// <returns>This collection, with <paramref name="right" /> appended.</returns>
    public FlowMetaCollection Merge(FlowMeta right)
    {
        Push((FlowMetaNode)right);
        return this;
    }

    /// <summary>
    ///     Resolves the chain back-to-front: creates the terminal sink value(s) (a push sink produces its
    ///     own terminal producer, a composite hands out its push input and pull output), wraps the
    ///     push-push pipes on both sides of the composite, then invokes the source with the wrapped target
    ///     and feeds the hybrid pipes (consumator→producator) with the pull side and the post-composite
    ///     target.
    /// </summary>
    /// <param name="context">The flow context passed to the handlers.</param>
    public void Build(FlowContext context)
    {
        Trace.WriteLine("[FlowMetaCollection] Build: resolving chain");

        // Drain the stack into a list: index 0 is the source, the last is the sink.
        var nodes = new FlowMetaNode[_stack.Count];
        for (var i = nodes.Length - 1; i >= 0; i--)
            nodes[i] = _stack.Pop();

        if (nodes.Length == 0)
            return;

        // 1. Create the composite bridge (push input written by the upstream push chain, pull output read
        //    by the downstream hybrid pipe or pull sink) and the terminal sink value. A pull sink consumes
        //    the composite's pull output; a push sink creates the terminal producer it consumes.
        var composite = FindComposite(nodes);
        object? pullOutput = composite?.Invoke(context);

        object? sinkProducer = null;
        for (var i = nodes.Length - 1; i >= 1; i--)
        {
            if (nodes[i].Kind != NodeKind.Sink)
                continue;

            sinkProducer = IsPullSink(nodes[i])
                ? pullOutput
                : nodes[i].Invoke(context);
            break;
        }

        // 2. Determine whether the source is a push source (accepts a plain producator target) or a
        //    pull source (emits a consumator/enumerator through an out parameter).
        var pushSource = nodes[0].Kind == NodeKind.Source && nodes[0].IsPushSource;

        // 3. Wrap the push-push pipes into two separate chains. The pre-composite pipes (between the
        //    source and the composite) wrap the composite's push input. The post-hybrid pipes (between the
        //    hybrid consumator→producator pipe and the sink) wrap the terminal producer the sink created.
        //    Without a composite there is no pre-chain; without a hybrid there is no post-chain (all pipes
        //    then wrap the terminal producer as a single pre-chain).
        var compositeIndex = composite is null ? -1 : Array.IndexOf(nodes, composite);
        var hybridIndex = FindLastHybrid(nodes, composite);

        object? postTarget = sinkProducer;
        if (hybridIndex >= 0)
        {
            // Downstream of the hybrid: innermost pipe (closest to the sink) wraps the producer first.
            for (var i = nodes.Length - 1; i > hybridIndex; i--)
            {
                if (nodes[i].Kind != NodeKind.Pipe || nodes[i].InType is null || !IsPushPipe(nodes[i]))
                    continue;

                postTarget = nodes[i].InvokePipe(context, null, postTarget);
            }
        }
        else if (compositeIndex >= 0)
        {
            // No hybrid: pipes downstream of the composite (none in a pull-sink chain) would wrap the
            // producer; there are none, so the post-target stays the terminal producer.
        }

        object? preTarget;
        if (compositeIndex >= 0)
        {
            // Upstream of the composite: innermost pipe (closest to the composite) wraps the push input first.
            preTarget = composite!.PushInput;
            for (var i = compositeIndex - 1; i >= 1; i--)
            {
                if (nodes[i].Kind != NodeKind.Pipe || nodes[i].InType is null || !IsPushPipe(nodes[i]))
                    continue;

                preTarget = nodes[i].InvokePipe(context, null, preTarget);
            }
        }
        else if (hybridIndex < 0)
        {
            // A single push chain (no composite, no hybrid): every pipe wraps the terminal producer.
            preTarget = sinkProducer;
            for (var i = nodes.Length - 1; i >= 1; i--)
            {
                if (nodes[i].Kind != NodeKind.Pipe || nodes[i].InType is null || !IsPushPipe(nodes[i]))
                    continue;

                preTarget = nodes[i].InvokePipe(context, null, preTarget);
            }
        }
        else
        {
            // A hybrid without a composite is fed by a pull source — there is no pre-chain to feed.
            preTarget = postTarget;
        }

        // 4. Invoke the source once: a push source receives the wrapped pre-composite target (and starts
        //    pushing), a pull source produces the upstream consumator/enumerator. A pull chain fused with
        //    its pull-pull pipes already carries the produced stream in Value — re-invoking the merged
        //    node would construct the pipes a second time.
        object? upstream;
        if (nodes[0].Kind == NodeKind.Pipe && nodes[0].Value is not null)
        {
            upstream = nodes[0].Value;
        }
        else
        {
            upstream = nodes[0].Invoke(context, pushSource ? preTarget : null);
        }

        // 5. Feed the pull sink with the consumator stream produced by the composite.
        if (composite is not null)
        {
            for (var i = nodes.Length - 1; i >= 1; i--)
            {
                if (nodes[i].Kind != NodeKind.Sink || !IsPullSink(nodes[i]))
                    continue;

                nodes[i].Invoke(context, pullOutput);
                break;
            }
        }

        // 6. Feed the hybrid pipes (consumator→producator): they read the pull side (the composite's pull
        //    output, or the pull source's consumator) and push into the post-composite target.
        for (var i = nodes.Length - 1; i >= 1; i--)
        {
            if (nodes[i].Kind != NodeKind.Pipe || IsPushPipe(nodes[i]))
                continue;

            if (composite is not null && ReferenceEquals(nodes[i], composite))
                continue;

            nodes[i].InvokePipe(context, pullOutput ?? upstream, postTarget);
        }

        Trace.WriteLine("[FlowMetaCollection] Build: chain resolved");
    }

    /// <summary>
    ///     Returns the index of the last hybrid pipe (consumator→producator, e.g. the Warming processor)
    ///     in the collection, or <c>-1</c> when there is none. Hybrid pipes are the only non-composite,
    ///     non-push-push pipes that appear in a push chain.
    /// </summary>
    private static int FindLastHybrid(FlowMetaNode[] nodes, FlowMetaNode? composite)
    {
        for (var i = nodes.Length - 1; i >= 1; i--)
        {
            if (nodes[i].Kind != NodeKind.Pipe)
                continue;

            if (composite is not null && ReferenceEquals(nodes[i], composite))
                continue;

            if (!IsPushPipe(nodes[i]))
                return i;
        }

        return -1;
    }

    /// <summary>
    ///     Returns <see langword="true" /> for a sink that consumes a pull interface (a consumator or
    ///     enumerator, e.g. a reader fed by a composite's pull output); <see langword="false" /> for a push
    ///     sink that creates the terminal producer through an <see langword="out" /> parameter.
    /// </summary>
    private static bool IsPullSink(FlowMetaNode node)
    {
        var parameters = node.Method.GetParameters();

        foreach (var parameter in parameters)
        {
            if (parameter.ParameterType == typeof(FlowContext))
                continue;

            if (parameter.IsOut)
                return false;

            var type = parameter.ParameterType;
            if (type.IsGenericType)
                type = type.GetGenericTypeDefinition();

            return type == typeof(Abstractions.IAsyncConsumator<>) || type == typeof(Abstractions.IConsumator<>) ||
                   type == typeof(System.Collections.Generic.IAsyncEnumerator<>) || type == typeof(System.Collections.Generic.IEnumerator<>);
        }

        return false;
    }

    /// <summary>
    ///     Returns the composite push→pull pipe (<c>Fuse(out IProducator, out IConsumator, ctx)</c>) in the
    ///     collection, or <see langword="null" /> when there is none. A composite node exposes both a push
    ///     input producer (written by the upstream push source) and a pull output stream (read by the
    ///     downstream consumator).
    /// </summary>
    private static FlowMetaNode? FindComposite(FlowMetaNode[] nodes)
    {
        for (var i = nodes.Length - 1; i >= 1; i--)
        {
            if (nodes[i].Kind == NodeKind.Pipe && nodes[i].Method.GetParameters().Length >= 3 &&
                IsComposite(nodes[i]))
                return nodes[i];
        }

        return null;
    }

    /// <summary>Returns <see langword="true" /> for a composite pipe: both flow parameters are out (emitted).</summary>
    private static bool IsComposite(FlowMetaNode node)
    {
        var parameters = node.Method.GetParameters();
        var flowCount = 0;
        var outCount = 0;

        foreach (var parameter in parameters)
        {
            if (parameter.ParameterType == typeof(FlowContext))
                continue;

            flowCount++;
            if (parameter.IsOut)
                outCount++;
        }

        return flowCount == 2 && outCount == 2;
    }

    /// <summary>
    ///     Returns <see langword="true" /> for a pure push-push pipe (producator→producator), whose input
    ///     producer is implemented by the node itself (an out parameter) and whose second parameter is the
    ///     plain downstream target.
    /// </summary>
    private static bool IsPushPipe(FlowMetaNode node)
    {
        var method = node.Method;
        var parameters = method.GetParameters();
        if (parameters.Length < 2)
            return false;

        var first = parameters[0];
        var second = parameters[1];

        // A push-push pipe is declared as Fuse(out IProducator<T1> source, IProducator<T2> target, ctx):
        // the first parameter is the out source (the input producer the node implements itself) and the
        // second is the plain downstream target.
        return first.ParameterType != typeof(FlowContext) &&
               second.ParameterType != typeof(FlowContext) &&
               first.IsOut && !second.IsOut &&
               IsProducator(first.ParameterType) && IsProducator(second.ParameterType);
    }

    private static bool IsProducator(Type type)
    {
        type = type.IsByRef ? type.GetElementType()! : type;
        if (type.IsGenericType)
            type = type.GetGenericTypeDefinition();
        return type == typeof(Abstractions.IProducator<>) || type == typeof(Abstractions.IAsyncProducator<>);
    }
}
