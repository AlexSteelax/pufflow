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
    ///     Resolves the chain back-to-front: creates the terminal producator target (from the sink), wraps
    ///     it through the push-push pipes (each pipe receives the current target and returns its own out
    ///     source — the input producer it implements itself — as the new target), then invokes the source
    ///     with the wrapped target and feeds the hybrid pipes (consumator→producator) with both the
    ///     upstream and the terminal target.
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

        // 1. Invoke the sink: it produces the terminal producator (target) for a push sink, or expects
        //    a consumator input (produced by the composite) for a pull sink. A composite pipe
        //    (Fuse(out IProducator, out IConsumator, ctx)) that precedes the sink is invoked first so it
        //    creates the consumator stream the sink consumes.
        object? target = null;
        var composite = FindComposite(nodes);
        for (var i = nodes.Length - 1; i >= 1; i--)
        {
            if (nodes[i].Kind != NodeKind.Sink)
                continue;

            target = composite is not null
                ? composite.Invoke(context) // composite creates the consumator; Invoke returns its pull output
                : nodes[i].Invoke(context);
            break;
        }

        // 2. Determine whether the source is a push source (accepts a plain producator target) or a
        //    pull source (emits a consumator/enumerator through an out parameter).
        var pushSource = nodes[0].Kind == NodeKind.Source && nodes[0].IsPushSource;

        // 3. Wrap the terminal target through the push-push pipes in reverse order: each pipe
        //    (producator→producator) receives the current target and returns its own out source (the
        //    input producer it implements itself) as the new target. Hybrid pipes (consumator→producator)
        //    are invoked later with both the upstream and the terminal target. A composite push→pull pipe
        //    feeds its consumator output to the sink and its producator input to the push source.
        object? pipeTarget = target;
        for (var i = nodes.Length - 1; i >= 1; i--)
        {
            if (nodes[i].Kind != NodeKind.Pipe)
                continue;

            if (composite is not null && ReferenceEquals(nodes[i], composite))
                continue;

            if (nodes[i].InType is not null && IsPushPipe(nodes[i]))
            {
                // Push-push pipe: it owns the input producer; the out source it returns becomes the
                // target fed to the source / the previous pipe.
                pipeTarget = nodes[i].InvokePipe(context, null, pipeTarget);
            }
        }

        // 4. Invoke the source once: a push source receives the wrapped target (and starts pushing), a
        //    pull source produces the upstream consumator/enumerator. A composite push→pull pipe hands
        //    its producator input to the push source as the write target.
        object? upstream = composite is not null && pushSource
            ? nodes[0].Invoke(context, composite.PushInput)
            : nodes[0].Invoke(context, pushSource ? pipeTarget : null);

        // 5. Feed the consumator stream (produced by the composite) into the pull sink.
        if (composite is not null)
        {
            for (var i = nodes.Length - 1; i >= 1; i--)
            {
                if (nodes[i].Kind == NodeKind.Sink)
                {
                    nodes[i].Invoke(context, target);
                    break;
                }
            }
        }

        // 6. Feed both the upstream value and the terminal target into each hybrid pipe
        //    (consumator→producator) — these read the upstream pull side and push into the terminal.
        //    The terminal is the wrapped target (the input producer of the innermost push-push pipe),
        //    not the raw sink producer, so the hybrid pipe's output element type matches.
        for (var i = nodes.Length - 1; i >= 1; i--)
        {
            if (nodes[i].Kind != NodeKind.Pipe || IsPushPipe(nodes[i]))
                continue;

            if (composite is not null && ReferenceEquals(nodes[i], composite))
                continue;

            nodes[i].InvokePipe(context, upstream, pipeTarget);
        }

        Trace.WriteLine("[FlowMetaCollection] Build: chain resolved");
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
