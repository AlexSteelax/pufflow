using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Tests.Flows;

[Flow]
public partial class FlowPipeProducatorToProducator<T1, T2>(Func<T1, T2> transform)
{
    public void Fuse(out IProducator<T1> source, IProducator<T2> target, FlowContext context)
    {
        source = new Producator(target, transform);
    }

    private sealed class Producator(IProducator<T2> target, Func<T1, T2> transform) : IProducator<T1>
    {
        public bool TryWrite(T1 value) => target.TryWrite(transform.Invoke(value));

        public bool TryComplete(Exception? ex = null) => target.TryComplete(ex);
    }
}
