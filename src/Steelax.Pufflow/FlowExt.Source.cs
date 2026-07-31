using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow;

public static partial class FlowExt
{
    [PublicAPI]
    public static Source<T> Attach<T>(this IFlowable<Source<T>> flow, FlowSource source)
    {
        return new Source<T>(flow, source.Context);
    }
    
    [PublicAPI]
    public static Source<Tk, T> Attach<Tk, T>(this IFlowable<Source<Tk, T>> flow, FlowSource source)
    {
        return new Source<Tk, T>(flow, source.Context);
    }
}