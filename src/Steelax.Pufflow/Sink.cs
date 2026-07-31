namespace Steelax.Pufflow;

[PublicAPI]
public readonly struct Sink<T>(object instance, FlowContext context)
{
    internal readonly object Instance = instance;
    internal readonly FlowContext Context = context;
}

[PublicAPI]
public readonly struct Sink<TKind, T>(object instance, FlowContext context)
{
    internal readonly object Instance = instance;
    internal readonly FlowContext Context = context;
}