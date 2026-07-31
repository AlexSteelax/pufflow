namespace Steelax.Pufflow;

[PublicAPI]
public readonly struct Pipe<TLeft, TRight>(object instance, FlowContext context)
{
    internal readonly object Instance = instance;
    internal readonly FlowContext Context = context;
}

[PublicAPI]
public readonly struct Pipe<TKind, TLeft, TRight>(object instance, FlowContext context)
{
    internal readonly object Instance = instance;
    internal readonly FlowContext Context = context;
}