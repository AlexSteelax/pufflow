using System.Reflection;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow;

/// <summary>
///     Describes a single pipeline node: the handler method found on the node's type, the types it consumes
///     and produces, and its role in the chain (source/pipe/sink). Built once per node via reflection
///     (following the same <c>GetType</c> + <c>GetMethod</c> approach as <see cref="FlowMarshal" />), then
///     combined by <see cref="Merge" /> into connected nodes. The flow context is passed separately at
///     invocation time.
/// </summary>
internal sealed class FlowMetaNode : FlowMeta
{
    private readonly object _instance;
    private readonly MethodInfo _method;

    private FlowMetaNode(object instance, MethodInfo method, NodeKind kind, Type? inType, Type? outType)
    {
        _instance = instance;
        _method = method;
        Kind = kind;
        InType = inType;
        OutType = outType;
    }

    /// <summary>The role of this node in the chain.</summary>
    internal NodeKind Kind { get; }

    /// <summary>The type consumed by this node, or <see langword="null" /> for a source.</summary>
    internal Type? InType { get; }

    /// <summary>The type produced by this node, or <see langword="null" /> for a sink.</summary>
    internal Type? OutType { get; }

    /// <summary>The handler method found on the node (the unified <c>Fuse(...)</c> contract).</summary>
    internal MethodInfo Method => _method;

    /// <summary>The value produced by this node during the merge (an enumerator, a consumator, or a task for a sink).</summary>
    internal object? Value { get; private set; }

    /// <summary>
    ///     The push input producer handed out by a composite node (<c>Fuse(out IProducator, out IConsumator, ctx)</c>):
    ///     the first out parameter, fed to the upstream push source. <see langword="null" /> for non-composite nodes.
    /// </summary>
    internal object? PushInput { get; private set; }

    /// <summary>
    ///     Returns <see langword="true" /> for a push source (a <c>Fuse(IProducator target, ctx)</c> that
    ///     accepts a plain producator target and pushes into it), as opposed to a pull source that emits a
    ///     consumator/enumerator through an out parameter.
    /// </summary>
    internal bool IsPushSource
    {
        get
        {
            if (Kind != NodeKind.Source)
                return false;

            var parameters = _method.GetParameters();
            foreach (var p in parameters)
                if (p.ParameterType != typeof(FlowContext))
                    return !p.IsOut;
            return false;
        }
    }

    /// <summary>
    ///     Invokes the node's handler with the given upstream input (or without input for a source). The
    ///     value is fed into the first non-out parameter (the consumed input source).
    /// </summary>
    /// <param name="context">The flow context to pass to the handler.</param>
    /// <param name="input">The upstream value, or <see langword="null" /> for a source.</param>
    /// <returns>The handler result (an async enumerator/consumator for source/pipe, a <see cref="Task" /> for a sink).</returns>
    internal object? Invoke(FlowContext context, object? input = null)
    {
        Trace.WriteLine($"[FlowMeta] Invoke '{_method.Name}' on '{_instance.GetType().Name}' (in={InType?.Name ?? "-"}, out={OutType?.Name ?? "-"})");
        var parameters = _method.GetParameters();

        // The unified Fuse(...) contract: void Fuse(in/out IF..., FlowContext). The node assigns the
        // out parameters during the call; their boxed values are captured back and returned as the result.
        if (_method.Name == "Fuse")
        {
            var args = new object?[parameters.Length];
            var inputAssigned = false;

            for (var i = 0; i < parameters.Length; i++)
            {
                var parameter = parameters[i];

                if (parameter.ParameterType == typeof(FlowContext))
                {
                    args[i] = context;
                }
                else if (parameter.IsOut)
                {
                    // An out parameter — reflection fills the element after the call. Pass null for
                    // reference types; default(T) for value types (flow interfaces are reference types).
                    args[i] = null;
                }
                else if (!inputAssigned)
                {
                    // The first non-out flow parameter (plain or in-marked) receives the upstream value.
                    args[i] = input;
                    inputAssigned = true;
                }
                else
                {
                    args[i] = null;
                }
            }

            _method.Invoke(_instance, args);

            // The output value is the node's emitted flow interface (an out parameter); there may be
            // none (a pure sink). Capture the last non-context out parameter. A composite node
            // (Fuse(out IProducator, out IConsumator, ctx)) exposes two flow interfaces: the first out
            // is the push input producer (fed upstream), the last is the pull output stream (fed
            // downstream).
            object? result = null;
            for (var i = parameters.Length - 2; i >= 0; i--)
                if (parameters[i].IsOut)
                {
                    result = args[i];
                    break;
                }

            Value = result;

            // The first out flow parameter of a composite is the push input producer.
            PushInput = null;
            for (var i = 0; i < parameters.Length - 1; i++)
                if (parameters[i].IsOut && IsFlowParam(parameters[i]))
                {
                    PushInput = args[i];
                    break;
                }

            return result;
        }

        return input is null && parameters.Length == 1
            ? _method.Invoke(_instance, [context])
            : _method.Invoke(_instance, [input!, context]);
    }

    /// <summary>
    ///     Invokes a hybrid pipe (consumator→producator): the upstream consumator is fed into the first
    ///     non-out parameter, the downstream producator target into the last. Used by
    ///     <see cref="FlowMetaCollection.Build" /> for delayed (reverse-order) resolution, where the source
    ///     produces the upstream side and the sink produces the target.
    /// </summary>
    /// <param name="context">The flow context to pass to the handler.</param>
    /// <param name="upstream">The upstream consumator/enumerator produced by the source.</param>
    /// <param name="target">The downstream producator target produced by the sink.</param>
    internal object? InvokePipe(FlowContext context, object? upstream, object? target)
    {
        Trace.WriteLine($"[FlowMeta] InvokePipe '{_method.Name}' on '{_instance.GetType().Name}' (in={InType?.Name ?? "-"}, out={OutType?.Name ?? "-"})");
        var parameters = _method.GetParameters();
        var args = new object?[parameters.Length];
        var firstAssigned = false;
        var lastPlain = -1;

        for (var i = parameters.Length - 1; i >= 0; i--)
        {
            var p = parameters[i];
            if (p.ParameterType != typeof(FlowContext) && !p.IsOut)
            {
                lastPlain = i;
                break;
            }
        }

        for (var i = 0; i < parameters.Length; i++)
        {
            var parameter = parameters[i];

            if (parameter.ParameterType == typeof(FlowContext))
            {
                args[i] = context;
            }
            else if (parameter.IsOut)
            {
                args[i] = null;
            }
            else if (i == 0 && upstream is not null)
            {
                // The upstream value goes into the first non-out flow parameter: a pull consumator
                // (hybrid pipe) or, for a pure push pipe, the node's own input producer is not fed —
                // the node implements it itself, so upstream is not passed to the handler. Only the
                // pull input (consumator/enumerator) is fed into the first parameter; a push-push pipe
                // receives only the downstream target.
                if (IsPullParameter(parameters[0]))
                {
                    args[i] = upstream;
                    firstAssigned = true;
                }
            }
            else if (i == lastPlain)
            {
                // The downstream producator target (from the sink) goes into the last parameter.
                args[i] = target;
            }
            else if (!firstAssigned)
            {
                args[i] = upstream;
                firstAssigned = true;
            }
            else
            {
                args[i] = null;
            }
        }

        _method.Invoke(_instance, args);

        // The pipe's emitted out parameter (the input producer it implements itself, for a push-push
        // pipe) becomes the new upstream/target for the previous node. Capture the last out parameter.
        object? result = null;
        for (var i = parameters.Length - 2; i >= 0; i--)
            if (parameters[i].IsOut)
            {
                result = args[i];
                break;
            }

        Value = result;
        return result;
    }

    /// <summary>
    ///     Builds the <see cref="FlowMeta" /> for a node instance via reflection. The flow shape is passed
    ///     explicitly as <see cref="FlowKind" /> flags (derived from the statically-known marker, e.g.
    ///     <c>IFlowable{Pipe{IAsyncConsumator{T1}, IAsyncConsumator{T2}}}</c>), so the
    ///     node kind (source/pipe/sink) and the handler method are resolved deterministically instead of
    ///     scanning all methods on the type.
    /// </summary>
    /// <param name="instance">The node instance (a <c>[Flow]</c>-annotated component).</param>
    /// <param name="inKind">The flow interface the node consumes (<see cref="FlowKind.None" /> for a source).</param>
    /// <param name="outKind">The flow interface the node produces (<see cref="FlowKind.None" /> for a sink).</param>
    /// <returns>The node's meta, ready to be combined via <see cref="Merge" /> or a <see cref="FlowMetaCollection" />.</returns>
    /// <exception cref="FlowMetaException">
    ///     Thrown when no supported handler matches the requested flow shape on the node.
    /// </exception>
    internal static FlowMeta Create(object instance, FlowKind inKind, FlowKind outKind)
    {
        // Backwards-compatible wrapper: builds the ordered kinds (input without Out, output with Out).
        return inKind == FlowKind.None
            ? Create(instance, [outKind | FlowKind.Out])
            : Create(instance, [inKind, outKind | FlowKind.Out]);
    }

    /// <summary>
    ///     Builds the <see cref="FlowMeta" /> for a node instance via reflection. The flow shape is passed
    ///     explicitly as an ordered list of <see cref="FlowKind" /> flags — one per flow interface of the
    ///     node (the first is the input side, the last the output side). Each kind carries the
    ///     <see cref="FlowKind.Out" /> flag when the node owns/emits that interface (a <c>Fuse(out ...)</c>
    ///     parameter) and lacks it when the node consumes it (a <c>Fuse(in ...)</c> parameter).
    /// </summary>
    /// <param name="instance">The node instance (a <c>[Flow]</c>-annotated component).</param>
    /// <param name="kinds">
    ///     The ordered flow interface kinds of the node. A single kind with <see cref="FlowKind.Out" /> is a
    ///     source; a single kind without it is a sink; two kinds (input, then output) form a pipe.
    /// </param>
    /// <returns>The node's meta, ready to be combined via <see cref="Merge" /> or a <see cref="FlowMetaCollection" />.</returns>
    /// <exception cref="FlowMetaException">
    ///     Thrown when no supported handler matches the requested flow shape on the node.
    /// </exception>
    internal static FlowMeta Create(object instance, params FlowKind[] kinds)
    {
        var type = instance.GetType();
        var (inKind, outKind) = kinds.Length switch
        {
            0 => (FlowKind.None, FlowKind.None),
            1 => kinds[0].HasFlag(FlowKind.Out)
                ? (FlowKind.None, kinds[0] & ~FlowKind.Out)
                : (kinds[0], FlowKind.None),
            _ => (kinds[0], kinds[^1] & ~FlowKind.Out)
        };

        Trace.WriteLine($"[FlowMeta] Create: type='{type.Name}' in='{inKind}' out='{outKind}'");

        // The node kind is derived from the flow shape, not passed by the caller. A single flow
        // interface is classified by the actual Fuse parameter modifier (a push-source accepts a plain
        // target, a write-sink hands out an out target, a read-source emits an out stream, a read-sink
        // consumes an in stream); two interfaces form a pipe; a sole input forms a sink.
        var nodeKind = inKind == FlowKind.None
            ? ClassifySingle(type, outKind)
            : outKind == FlowKind.None ? NodeKind.Sink : NodeKind.Pipe;

        var method = ResolveHandler(type, nodeKind, inKind, outKind)
            ?? throw new FlowMetaException(
                $"No supported flow handler found on '{type.Name}' for in='{inKind}', out='{outKind}'.");

        var (inType, outType) = ResolveTypes(method, nodeKind, inKind, outKind);
        Trace.WriteLine($"[FlowMeta] Create {nodeKind}: '{type.Name}' in='{inType?.Name ?? "-"}' out='{outType?.Name ?? "-"}'");
        return new FlowMetaNode(instance, method, nodeKind, inType, outType);
    }

    /// <summary>
    ///     Combines two nodes into a connected node. Accepts any <see cref="FlowMeta" />: a
    ///     <see cref="FlowMetaNode" /> is merged via pull semantics (the upstream value is produced and
    ///     passed into the downstream handler), while a <see cref="FlowMetaCollection" /> appends the right
    ///     node for later reverse-order (push) resolution.
    /// </summary>
    /// <param name="left">The upstream node.</param>
    /// <param name="right">
    ///     The downstream node, or <see langword="null" /> for a terminal (sink-only) connection.
    /// </param>
    /// <param name="context">The flow context of the resulting connection.</param>
    /// <returns>The connected node ready for further chaining or execution.</returns>
    /// <exception cref="FlowMetaException">
    ///     Thrown when the combination of the two node kinds is not supported or the types do not match.
    /// </exception>
    internal static FlowMeta Merge(FlowMeta left, FlowMeta? right, FlowContext context)
    {
        if (left is FlowMetaCollection collection)
        {
            if (right is null)
                throw new FlowMetaException("A flow collection requires a right node to append.");

            return collection.Merge(right);
        }

        return MergeNodes((FlowMetaNode)left, (FlowMetaNode?)right, context);
    }

    /// <summary>Pull-semantics merge of two concrete nodes.</summary>
    private static FlowMetaNode MergeNodes(FlowMetaNode left, FlowMetaNode? right, FlowContext context)
    {
        Trace.WriteLine($"[FlowMeta] Merge: left='{left.Kind}' out='{left.OutType?.Name ?? "-"}', right='{right?.Kind.ToString() ?? "null"}' in='{right?.InType?.Name ?? "-"}'");

        if (right is not null && left.OutType is not null && right.InType is not null &&
            left.OutType != right.InType)
        {
            throw new FlowMetaException(
                $"Type mismatch on the joint: left produces '{left.OutType}', right consumes '{right.InType}'.");
        }

        return (left.Kind, right?.Kind) switch
        {
            (NodeKind.Source, NodeKind.Pipe) => MergeSourcePipe(left, right!, context),
            (NodeKind.Pipe, NodeKind.Pipe) => MergePipePipe(left, right!, context),
            (NodeKind.Pipe, NodeKind.Sink) => MergePipeSink(left, right!, context),
            (NodeKind.Source, NodeKind.Sink) => MergeSourceSink(left, right!, context),
            (NodeKind.Source, null) => MergeSourceSink(left, null, context),
            _ => throw new FlowMetaException(
                $"Unsupported flow merge: left='{left.Kind}', right='{right?.Kind.ToString() ?? "null"}'.")
        };
    }

    /// <summary>Connects a source to a pipe: produces the source value and feeds it into the pipe.</summary>
    private static FlowMetaNode MergeSourcePipe(FlowMetaNode left, FlowMetaNode right, FlowContext context)
    {
        Trace.WriteLine("[FlowMeta] MergeSourcePipe");
        var input = left.Invoke(context);
        var result = right.Invoke(context, input);
        return new FlowMetaNode(right._instance, right.Method, NodeKind.Pipe, right.InType, right.OutType)
        {
            Value = result
        };
    }

    /// <summary>Connects two pipes: feeds the upstream pipe's value into the downstream pipe.</summary>
    private static FlowMetaNode MergePipePipe(FlowMetaNode left, FlowMetaNode right, FlowContext context)
    {
        Trace.WriteLine("[FlowMeta] MergePipePipe");
        // The left pipe already produced its output during the upstream merge (stored in Value); feed it
        // directly into the right pipe. Re-invoking the left handler would construct a second pipe
        // instance (and register a second background pump), processing the stream twice.
        var result = right.Invoke(context, left.Value);
        return new FlowMetaNode(right._instance, right.Method, NodeKind.Pipe, right.InType, right.OutType)
        {
            Value = result
        };
    }

    /// <summary>Connects a pipe to a sink: feeds the pipe's value into the sink's Fuse.</summary>
    private static FlowMetaNode MergePipeSink(FlowMetaNode left, FlowMetaNode right, FlowContext context)
    {
        Trace.WriteLine("[FlowMeta] MergePipeSink");
        // The pipe already produced its output value during the upstream merge (MergeSourcePipe /
        // MergePipePipe stored it in Value). Re-invoking the pipe handler would resolve the upstream
        // source a second time and attempt to cast its output to the pipe's input type, so the sink
        // receives the cached Value directly.
        right.Invoke(context, left.Value);
        return new FlowMetaNode(right._instance, right.Method, NodeKind.Sink, right.InType, right.OutType);
    }

    /// <summary>Connects a source to a sink (or to null): feeds the source value into the sink.</summary>
    private static FlowMetaNode MergeSourceSink(FlowMetaNode left, FlowMetaNode? right, FlowContext context)
    {
        Trace.WriteLine($"[FlowMeta] MergeSourceSink: right='{right?.Kind.ToString() ?? "null"}'");
        var input = left.Invoke(context);
        if (right is null)
        {
            // Terminal source: no downstream consumer — drain the source enumerator.
            var drain = typeof(FlowMetaNode).GetMethod(nameof(DrainAsync), BindingFlags.NonPublic | BindingFlags.Static)!;
            _ = DrainAsync(context, (IAsyncEnumerator<object>)input!);
            return new FlowMetaNode(left._instance, drain, NodeKind.Sink, left.InType, null);
        }

        right.Invoke(context, input); // Fuse registers its task during the merge.
        return new FlowMetaNode(right._instance, right.Method, NodeKind.Sink, right.InType, right.OutType);
    }

    private static async Task DrainAsync(FlowContext context, IAsyncEnumerator<object> enumerator)
    {
        await using (enumerator.ConfigureAwait(false))
            while (await enumerator.MoveNextAsync().ConfigureAwait(false))
            {
                // Drain the upstream enumerator without a downstream consumer.
            }
    }

    /// <summary>
    ///     Resolves the unified <c>Fuse(...)</c> handler for the requested flow shape. Each flow
    ///     parameter direction must match the node role: the input interface is consumed (non-out), the
    ///     output interface is emitted (<see langword="out" />) — with the single-interface push-family
    ///     exception where a source pushes into a plain target and a sink hands out an <see langword="out" />
    ///     target.
    /// </summary>
    private static MethodInfo? ResolveHandler(Type type, NodeKind nodeKind, FlowKind inKind, FlowKind outKind)
    {
        var inFlow = GetFlowInterface(inKind);
        var outFlow = GetFlowInterface(outKind);

        return nodeKind switch
        {
            // Source: a push source pushes into a plain target (Fuse(IProducator, ctx)); a pull source
            // emits a read stream (Fuse(out IF, ctx)).
            NodeKind.Source when outFlow is not null && IsPush(outKind) =>
                FindFuse(type, new[] { false }, new[] { outFlow }),

            NodeKind.Source when outFlow is not null =>
                FindFuse(type, new[] { true }, new[] { outFlow }),

            // Sink: a write sink hands out an out target to be written into (Fuse(out IProducator, ctx));
            // a read sink consumes an in source (Fuse(in IF, ctx)). For a push-family sink the target
            // is handed out as an out parameter, so it is emitted.
            NodeKind.Sink when inFlow is not null && IsPush(inKind) =>
                FindFuse(type, new[] { true }, new[] { inFlow }),

            NodeKind.Sink when inFlow is not null =>
                FindFuse(type, new[] { false }, new[] { inFlow }),

            // Pipe consumes an input and produces an output. The output parameter direction follows the
            // interface families: a pull output (consumator/enumerator) is emitted as an out parameter
            // ([false, true]); a push output (producator) is emitted as an out parameter for a push pipe
            // (producator→producator, [false, true]) but consumed as a plain target for a pull pipe
            // (consumator→producator, [false, false] — the node pushes into the supplied target).
            //
            // A pure push pipe (producator→producator) is declared with the out source first and the plain
            // target second — Fuse(out IProducator<T1> source, IProducator<T2> target, ctx) — where the
            // node owns the input producer (the upstream value flows into it through its own TryWrite) and
            // the plain second parameter is the downstream target. The emitted flags are therefore
            // [true, false] (source emitted via out, target consumed as a plain parameter).
            NodeKind.Pipe when inFlow is not null && outFlow is not null && IsPush(inKind) && IsPush(outKind) =>
                FindFuse(type, new[] { true, false }, new[] { inFlow, outFlow }),

            // Composite push→pull (a passive buffer bridge): Fuse(out IProducator<T1> source, out IConsumator<T2> target, ctx)
            // exposes both flow interfaces as out parameters — the push input producer (written by the upstream push
            // source) and the pull output stream (read by the downstream consumator). Both interfaces are emitted.
            NodeKind.Pipe when inFlow is not null && outFlow is not null && IsPush(inKind) && !IsPush(outKind) =>
                FindFuse(type, new[] { true, true }, new[] { inFlow, outFlow }),

            NodeKind.Pipe when inFlow is not null && outFlow is not null =>
                FindFuse(type, new[] { false, !(IsPush(outKind) && !IsPush(inKind)) }, new[] { inFlow, outFlow }),

            _ => null,
        };
    }

    /// <summary>
    ///     Classifies a node exposing a single flow interface. The role is decided by the actual Fuse
    ///     parameter modifier, since the legacy two-kind overload cannot carry the direction:
    ///     a plain target (Fuse(IProducator, ctx)) is a push source, an out write target
    ///     (Fuse(out IProducator, ctx)) is a passive sink, an out read stream
    ///     (Fuse(out IConsumator, ctx)) is a source and an in read stream (Fuse(in IConsumator, ctx))
    ///     is a sink.
    /// </summary>
    private static NodeKind ClassifySingle(Type type, FlowKind kind)
    {
        var flow = GetFlowInterface(kind);

        if (flow is null)
            return NodeKind.Sink;

        if ((kind & FlowKind.FamilyMask) == FlowKind.Producator)
        {
            // Push family: the node is a Source when it accepts a plain write target and pushes into it,
            // a Sink when it hands out an out target to be written into (a passive consumer).
            var plainFuse = FindFuse(type, new[] { false }, new[] { flow });
            return plainFuse is null ? NodeKind.Sink : NodeKind.Source;
        }

        // Pull family: the node is a Source when it emits a read stream (out), a Sink when it consumes
        // a supplied read stream (in/plain).
        var emittedFuse = FindFuse(type, new[] { true }, new[] { flow });
        return emittedFuse is null ? NodeKind.Sink : NodeKind.Source;
    }

    /// <summary>
    ///     Resolves the consumed and produced element types from the handler's signature. For the unified
    ///     <c>Fuse(...)</c> contract the interfaces live in the parameters (the first flow parameter is the
    ///     input side, the last the output side); the legacy <c>Get*</c> handlers carry the output in the
    ///     return value.
    /// </summary>
    private static (Type? In, Type? Out) ResolveTypes(MethodInfo method, NodeKind nodeKind, FlowKind inKind, FlowKind outKind)
    {
        var inFlow = GetFlowInterface(inKind);
        var outFlow = GetFlowInterface(outKind);
        var isFuse = method.Name == "Fuse";

        // The output interface of a Fuse handler is its last flow parameter; for Get* it is the return
        // value. The input interface is the first flow parameter for both.
        var parameters = method.GetParameters();
        var outSource = outFlow is not null
            ? isFuse
                ? GetInterfaceElementType(parameters[^2].ParameterType, outFlow)
                : GetInterfaceElementType(method.ReturnType, outFlow)
            : null;
        var inSource = inFlow is not null
            ? GetInterfaceElementType(parameters[0].ParameterType, inFlow)
            : null;

        return nodeKind switch
        {
            NodeKind.Source when IsPush(outKind) =>
                // Push source: the (plain) target parameter carries the element type.
                (null, outSource),

            NodeKind.Source =>
                // Pull source: the emitted stream carries the element type.
                (null, outSource),

            NodeKind.Sink when IsPush(inKind) =>
                // Push sink: the returned/out target producer carries the element type.
                (inSource, null),

            NodeKind.Sink =>
                // Pull sink: the consumed in stream carries the element type.
                (inSource, null),

            // Pipe: pull reads the input from the first parameter and produces the output; push is
            // reversed (the downstream target is the parameter, the upstream producer is the return).
            _ when IsPush(outKind) =>
                (inSource, outSource),

            _ =>
                (inSource, outSource),
        };
    }

    /// <summary>Returns <see langword="true" /> when the flow kind belongs to the push (producator) family.</summary>
    private static bool IsPush(FlowKind kind) => (kind & FlowKind.FamilyMask) == FlowKind.Producator;

    /// <summary>Returns the open generic flow interface type for the given flow kind, or <see langword="null" />.</summary>
    private static Type? GetFlowInterface(FlowKind kind)
    {
        var async = (kind & FlowKind.Async) != 0;

        return (kind & FlowKind.FamilyMask) switch
        {
            FlowKind.Enumerator => async ? typeof(IAsyncEnumerator<>) : typeof(IEnumerator<>),
            FlowKind.Consumator => async ? typeof(IAsyncConsumator<>) : typeof(IConsumator<>),
            FlowKind.Producator => async ? typeof(IAsyncProducator<>) : typeof(IProducator<>),
            _ => null,
        };
    }

    /// <summary>
    ///     Finds the unified <c>Fuse(...)</c> handler whose flow parameters match the requested
    ///     <see cref="FlowKind" /> interfaces and per-parameter directions (<see langword="true" /> when
    ///     the node emits that interface as an <see langword="out" /> parameter, <see langword="false" />
    ///     when it consumes it as a plain/input parameter). The method takes the flow parameters followed
    ///     by a <see cref="FlowContext" /> (a total of <c>interfaces.Length + 1</c> parameters).
    /// </summary>
    private static MethodInfo? FindFuse(Type type, bool[] emitted, Type[] interfaces)
    {
        var parameterCount = interfaces.Length + 1;

        foreach (var method in type.GetMethods(BindingFlags.Public | BindingFlags.Instance))
        {
            if (method.Name != "Fuse" || method.GetParameters().Length != parameterCount)
                continue;

            var parameters = method.GetParameters();
            var matches = true;

            for (var i = 0; i < interfaces.Length; i++)
            {
                var parameter = parameters[i];

                // An input interface is any non-out parameter (plain or in-marked); an output interface
                // must be an out parameter.
                if (parameter.IsOut != emitted[i])
                {
                    matches = false;
                    break;
                }

                // The parameter must be exactly the requested flow interface (after stripping a ref/out
                // wrapper). An inheritance match is intentionally rejected: IAsyncProducator{T} derives
                // from IProducator{T}, so a synchronous request for IProducator{} must not bind to an
                // async handler (and vice versa) — otherwise a node exposing both overloads would resolve
                // the wrong one.
                if (!IsExactGenericInterface(parameter.ParameterType, interfaces[i]))
                {
                    matches = false;
                    break;
                }
            }

            if (matches)
                return method;
        }

        return null;
    }

    /// <summary>Strips the by-ref wrapper (a <c>ref</c>/<c>out</c> parameter) to expose the underlying type.</summary>
    private static Type UnwrapByRef(Type type) => type.IsByRef ? type.GetElementType()! : type;

    /// <summary>
    ///     Returns <see langword="true" /> when the parameter is a pull flow interface
    ///     (enumerator/consumator) — the input side of a hybrid consumator→producator pipe — and
    ///     <see langword="false" /> for a push (producator) interface.
    /// </summary>
    private static bool IsPullParameter(ParameterInfo parameter)
    {
        var type = UnwrapByRef(parameter.ParameterType);

        if (type.IsGenericType)
            type = type.GetGenericTypeDefinition();

        return type == typeof(IEnumerator<>) || type == typeof(IAsyncEnumerator<>) ||
               type == typeof(IConsumator<>) || type == typeof(IAsyncConsumator<>);
    }

    /// <summary>
    ///     Returns <see langword="true" /> when the parameter is any flow interface
    ///     (enumerator / consumator / producator), <see langword="false" /> for a <see cref="FlowContext" />.
    /// </summary>
    private static bool IsFlowParam(ParameterInfo parameter)
    {
        var type = UnwrapByRef(parameter.ParameterType);
        return type != typeof(FlowContext) && (IsPullParameter(parameter) || IsProducator(type));
    }

    /// <summary>Returns <see langword="true" /> when the type is a push (producator) interface.</summary>
    private static bool IsProducator(Type type)
    {
        type = UnwrapByRef(type);

        if (type.IsGenericType)
            type = type.GetGenericTypeDefinition();

        return type == typeof(IProducator<>) || type == typeof(IAsyncProducator<>);
    }

    /// <summary>
    ///     Returns <see langword="true" /> when the parameter type is exactly the requested generic flow
    ///     interface (after stripping a ref/out wrapper). Inherited interfaces are deliberately excluded:
    ///     <c>IAsyncProducator{T}</c> derives from <c>IProducator{T}</c>, so a sync request must not match
    ///     an async handler. A <c>Fuse(...)</c> parameter is always declared as a flow interface, so an
    ///     exact generic-definition comparison is sufficient.
    /// </summary>
    private static bool IsExactGenericInterface(Type type, Type interfaceType)
    {
        type = UnwrapByRef(type);
        return type.IsGenericType && type.GetGenericTypeDefinition() == interfaceType;
    }

    /// <summary>
    ///     Extracts the generic element type of an interface (e.g., <c>T</c> from
    ///     <c>IAsyncEnumerator</c>).
    /// </summary>
    private static Type? GetInterfaceElementType(Type type, Type interfaceType)
    {
        type = UnwrapByRef(type);

        if (type.IsGenericType && type.GetGenericTypeDefinition() == interfaceType)
            return type.GetGenericArguments()[0];

        var match = type.GetInterfaces()
            .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == interfaceType);
        return match?.GetGenericArguments()[0];
    }
}

/// <summary>The role of a node in the pipeline chain.</summary>
internal enum NodeKind
{
    /// <summary>Produces data (no input).</summary>
    Source,

    /// <summary>Consumes and produces data.</summary>
    Pipe,

    /// <summary>Consumes data (no output).</summary>
    Sink
}
