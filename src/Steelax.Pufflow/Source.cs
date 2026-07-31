namespace Steelax.Pufflow;

[PublicAPI]
public readonly struct Source<T>(object instance, FlowContext context)
{
    internal readonly object Instance = instance;
    internal readonly FlowContext Context = context;
}

[PublicAPI]
public readonly struct Source<TVoid, T>(object instance, FlowContext context)
{
    internal readonly object Instance = instance;
    internal readonly FlowContext Context = context;
}