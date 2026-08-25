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
    ///     Resolves the chain back-to-front. The chain alternates push segments with pull bridges:
    ///     <c>source → [push-push] → composite → [pull-pull] → hybrid → [push-push] → composite → … →
    ///     sink</c>. Every composite hands out a push input (written by the preceding push segment) and a
    ///     pull output (the start of the following pull segment); pull-pull pipes in a pull segment are
    ///     chained in order; every hybrid reads the end of its pull segment and pushes into the next push
    ///     segment. Push segments are wrapped in reverse order, pull segments are chained forward, then the
    ///     hybrids, the source and the sink are fed with the resolved streams.
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

        // 1. Classify the pipes: composites (push→pull), hybrids (pull→push) and pull-pull pipes
        //    (pull→pull). Push-push pipes belong to the push segments.
        var compositeIndexes = new List<int>();
        var hybridIndexes = new List<int>();
        var pullPipes = new HashSet<int>();
        for (var i = 1; i < nodes.Length; i++)
        {
            if (nodes[i].Kind != NodeKind.Pipe)
                continue;

            if (IsComposite(nodes[i]))
                compositeIndexes.Add(i);
            else if (IsPushPipe(nodes[i]))
                continue;
            else if (IsHybrid(nodes[i]))
                hybridIndexes.Add(i);
            else
                pullPipes.Add(i);
        }

        // 2. Invoke every composite: each hands out its push input (PushInput) and returns its pull output.
        var pullOutput = new object?[nodes.Length];
        foreach (var index in compositeIndexes)
            pullOutput[index] = nodes[index].Invoke(context);

        // 3. Create the terminal sink value: a push sink creates the terminal producer that ends the final
        //    push segment; a pull sink consumes the end of the final pull segment.
        object? terminal = null;
        var sinkIndex = -1;
        var pullSink = false;
        for (var i = nodes.Length - 1; i >= 1; i--)
        {
            if (nodes[i].Kind != NodeKind.Sink)
                continue;

            sinkIndex = i;
            pullSink = IsPullSink(nodes[i]);
            if (!pullSink)
                terminal = nodes[i].Invoke(context);
            break;
        }

        // 4. Wrap each push segment, from the last to the first. A push segment is the run of push-push
        //    pipes between the previous anchor (the source or a hybrid) and the segment's end — a
        //    composite's push input, or the push sink's terminal producer for the final segment. The
        //    innermost pipe (closest to the segment end) wraps first.
        object? pushTarget = terminal;
        if (terminal is not null)
        {
            var lastHybrid = hybridIndexes.Count > 0 ? hybridIndexes[^1] : 0;
            pushTarget = WrapPushPipes(nodes, lastHybrid, sinkIndex, terminal, context);
        }

        var pushInput = new object?[nodes.Length];
        for (var k = compositeIndexes.Count - 1; k >= 0; k--)
        {
            var cIndex = compositeIndexes[k];
            var start = PreviousAnchor(hybridIndexes, cIndex);
            pushInput[cIndex] = WrapPushPipes(nodes, start, cIndex, nodes[cIndex].PushInput, context);
        }

        // 5. Produce the source side: a push source is fed with the first push segment's target; a pull
        //    source (or a pull chain already fused into a node) produces the upstream stream consumed by
        //    the first pull segment when no composite precedes it.
        var sourceTarget = compositeIndexes.Count > 0 ? pushInput[compositeIndexes[0]] : pushTarget;

        object? upstream = null;
        if (nodes[0].Kind == NodeKind.Source && nodes[0].IsPushSource)
        {
            nodes[0].Invoke(context, sourceTarget);
        }
        else if (nodes[0].Kind == NodeKind.Pipe && nodes[0].Value is not null)
        {
            // A merged pull chain carries its produced stream in Value — re-invoking the node would
            // construct the pipes a second time.
            upstream = nodes[0].Value;
        }
        else
        {
            upstream = nodes[0].Invoke(context, null);
        }

        // 6. Resolve each pull segment forward: it starts at the preceding composite's pull output (or
        //    the pull source's stream), chains the pull-pull pipes in order, and ends at a hybrid's input
        //    (or the pull sink).
        var hybridInput = new object?[nodes.Length];

        foreach (var hIndex in hybridIndexes)
        {
            var prevComposite = PreviousAnchor(compositeIndexes, hIndex);
            var stream = prevComposite >= 0 ? pullOutput[prevComposite] : upstream;
            var start = prevComposite >= 0 ? prevComposite : 0;

            hybridInput[hIndex] = ChainPullPipes(nodes, pullPipes, start, hIndex, stream, context);
        }

        // 7. Feed the pull sink with the end of the final pull segment.
        if (pullSink)
        {
            var lastComposite = compositeIndexes.Count > 0 ? compositeIndexes[^1] : -1;
            var stream = lastComposite >= 0 ? pullOutput[lastComposite] : upstream;
            var start = lastComposite >= 0 ? lastComposite : 0;

            stream = ChainPullPipes(nodes, pullPipes, start, sinkIndex, stream, context);
            nodes[sinkIndex].Invoke(context, stream);
        }

        // 8. Feed the hybrids: each reads the end of its pull segment and pushes into the next push
        //    segment (or the final push segment).
        foreach (var hIndex in hybridIndexes)
        {
            var nextComposite = -1;
            foreach (var cIndex in compositeIndexes)
            {
                if (cIndex < hIndex)
                    continue;
                nextComposite = cIndex;
                break;
            }

            var nextTarget = nextComposite >= 0 ? pushInput[nextComposite] : pushTarget;
            nodes[hIndex].InvokePipe(context, hybridInput[hIndex], nextTarget);
        }

        Trace.WriteLine("[FlowMetaCollection] Build: chain resolved");
    }

    /// <summary>Returns the index of the last anchor (composite or hybrid) that precedes <paramref name="index" />, or <c>-1</c>.</summary>
    private static int PreviousAnchor(List<int> anchors, int index)
    {
        var previous = -1;
        foreach (var anchor in anchors)
        {
            if (anchor > index)
                break;
            previous = anchor;
        }

        return previous;
    }

    /// <summary>
    ///     Chains the pull-pull pipes with indices in <c>(start, end)</c>: each receives the previous
    ///     pipe's output stream (or <paramref name="stream" /> for the first) and returns its own output,
    ///     which becomes the next input.
    /// </summary>
    private static object? ChainPullPipes(FlowMetaNode[] nodes, HashSet<int> pullPipes, int start, int end, object? stream, FlowContext context)
    {
        for (var i = start + 1; i < end; i++)
        {
            if (!pullPipes.Contains(i))
                continue;

            stream = nodes[i].Invoke(context, stream);
        }

        return stream;
    }

    /// <summary>
    ///     Wraps the push-push pipes with indices in <c>(start, end)</c> around <paramref name="target" />,
    ///     from the innermost pipe (closest to <paramref name="end" />) to the outermost, returning the
    ///     outermost pipe's out source as the new target for the preceding stage.
    /// </summary>
    private static object? WrapPushPipes(FlowMetaNode[] nodes, int start, int end, object? target, FlowContext context)
    {
        for (var i = end - 1; i > start; i--)
        {
            if (nodes[i].Kind != NodeKind.Pipe || nodes[i].InType is null || !IsPushPipe(nodes[i]))
                continue;

            target = nodes[i].InvokePipe(context, null, target);
        }

        return target;
    }

    /// <summary>Returns <see langword="true" /> for a hybrid pipe: a plain pull input and a plain push output.</summary>
    private static bool IsHybrid(FlowMetaNode node)
    {
        var parameters = node.Method.GetParameters();
        if (parameters.Length < 3)
            return false;

        var first = parameters[0];
        var second = parameters[1];

        return first.ParameterType != typeof(FlowContext) &&
               second.ParameterType != typeof(FlowContext) &&
               !first.IsOut && !second.IsOut &&
               IsPull(first.ParameterType) && IsProducator(second.ParameterType);
    }

    private static bool IsPull(Type type)
    {
        type = type.IsByRef ? type.GetElementType()! : type;
        if (type.IsGenericType)
            type = type.GetGenericTypeDefinition();

        return type == typeof(Abstractions.IConsumator<>) || type == typeof(Abstractions.IAsyncConsumator<>) ||
               type == typeof(System.Collections.Generic.IEnumerator<>) || type == typeof(System.Collections.Generic.IAsyncEnumerator<>);
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
