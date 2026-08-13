using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow;

/// <summary>
///     Describes the flow interface shape of a pipeline node: whether it is async, whether the node
///     owns/emits it (the <see cref="FlowKind.Out" /> flag) and which flow family
///     (enumerator / consumator / producator) it belongs to. Used by <c>FlowMetaNode.Create</c> to
///     resolve the handler method deterministically from the statically-known marker shape (e.g.
///     <c>IFlowable{Pipe{IAsyncConsumator{T1}, IAsyncConsumator{T2}}}</c>) instead of
///     scanning all methods on the type.
/// </summary>
[Flags]
internal enum FlowKind : byte
{
    /// <summary>No flow interface (a source input or a sink output).</summary>
    None = 0,

    /// <summary>Modifier flag: the flow interface is the async variant.</summary>
    Async = 1 << 0,

    /// <summary>Modifier flag: the node OWNS/emits this flow interface (a <c>Fuse(out ...)</c> parameter); without it the node consumes it (a <c>Fuse(in ...)</c> parameter).</summary>
    Out = 1 << 1,

    /// <summary>The flow family is an enumerator (pull).</summary>
    Enumerator = 1 << 2,

    /// <summary>The flow family is a consumator (pull).</summary>
    Consumator = 1 << 3,

    /// <summary>The flow family is a producator (push).</summary>
    Producator = 1 << 4,

    /// <summary>Synchronous <see cref="System.Collections.Generic.IEnumerator{T}" />.</summary>
    SyncEnumerator = Enumerator,

    /// <summary>Asynchronous <see cref="System.Collections.Generic.IAsyncEnumerator{T}" />.</summary>
    AsyncEnumerator = Async | Enumerator,

    /// <summary>Synchronous <see cref="IConsumator{T}" />.</summary>
    SyncConsumator = Consumator,

    /// <summary>Asynchronous <see cref="IAsyncConsumator{T}" />.</summary>
    AsyncConsumator = Async | Consumator,

    /// <summary>Synchronous <see cref="IProducator{T}" />.</summary>
    SyncProducator = Producator,

    /// <summary>Asynchronous <see cref="IAsyncProducator{T}" />.</summary>
    AsyncProducator = Async | Producator,

    // Emitted variants (a Fuse(out ...) parameter — the node owns this flow interface).
    /// <summary>Synchronous <see cref="IEnumerator{T}" /> emitted by the node.</summary>
    OutEnumerator = Out | Enumerator,

    /// <summary>Asynchronous <see cref="IAsyncEnumerator{T}" /> emitted by the node.</summary>
    OutAsyncEnumerator = Out | Async | Enumerator,

    /// <summary>Synchronous <see cref="IConsumator{T}" /> emitted by the node.</summary>
    OutConsumator = Out | Consumator,

    /// <summary>Asynchronous <see cref="IAsyncConsumator{T}" /> emitted by the node.</summary>
    OutAsyncConsumator = Out | Async | Consumator,

    /// <summary>Synchronous <see cref="IProducator{T}" /> emitted by the node.</summary>
    OutProducator = Out | Producator,

    /// <summary>Asynchronous <see cref="IAsyncProducator{T}" /> emitted by the node.</summary>
    OutAsyncProducator = Out | Async | Producator,

    /// <summary>Mask of the family bits (enumerator / consumator / producator).</summary>
    FamilyMask = Enumerator | Consumator | Producator,
}

/// <summary>Helpers for classifying runtime flow interface types into a <see cref="FlowKind" />.</summary>
internal static class FlowKindExtensions
{
    /// <summary>
    ///     Maps a flow interface type (e.g. <see cref="IAsyncConsumator{T}" />) to the corresponding
    ///     <see cref="FlowKind" /> flag, or <see cref="FlowKind.None" /> when the type is not a flow interface.
    /// </summary>
    /// <param name="type">The flow interface type to classify.</param>
    /// <returns>The matching <see cref="FlowKind" />, or <see cref="FlowKind.None" />.</returns>
    public static FlowKind Classify(this Type type)
    {
        var definition = type.IsGenericType ? type.GetGenericTypeDefinition() : type;

        return definition switch
        {
            _ when definition == typeof(IAsyncEnumerator<>) => FlowKind.AsyncEnumerator,
            _ when definition == typeof(IEnumerator<>) => FlowKind.SyncEnumerator,
            _ when definition == typeof(IAsyncConsumator<>) => FlowKind.AsyncConsumator,
            _ when definition == typeof(IConsumator<>) => FlowKind.SyncConsumator,
            _ when definition == typeof(IAsyncProducator<>) => FlowKind.AsyncProducator,
            _ when definition == typeof(IProducator<>) => FlowKind.SyncProducator,
            _ => FlowKind.None,
        };
    }
}
