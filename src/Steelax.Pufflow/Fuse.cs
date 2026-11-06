namespace Steelax.Pufflow;

/// <summary>
///     The live, partially composed chain. Carries a chain link ({@link ConsumatorChain},
///     {@link ProducatorChain} or their async variants) boxed as an <see cref="object" />, together with the
///     owning <see cref="FlowSource" />. The generic parameter is a phantom marker used only to keep the
///     composition type-safe; the actual delegate type is erased to <see cref="object" />.
/// </summary>
/// <typeparam name="TNext">The flow interface of the latest stage of the chain.</typeparam>
[PublicAPI]
public readonly struct Fuse<TNext>
{
    /// <summary>The boxed chain link of the latest stage.</summary>
    internal readonly object Next;

    /// <summary>The flow source owning the chain.</summary>
    internal readonly FlowSource FlowSource;

    /// <summary>Initializes a chain stage with its link and owning flow source.</summary>
    internal Fuse(FlowSource flowSource, object next)
    {
        FlowSource = flowSource;
        Next = next;
    }
}