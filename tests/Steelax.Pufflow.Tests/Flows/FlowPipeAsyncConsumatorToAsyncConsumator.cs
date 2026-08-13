using System.Diagnostics.CodeAnalysis;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Tests.Flows;

[Flow]
public partial class FlowPipeAsyncConsumatorToAsyncConsumator<T1, T2>(Func<T1, T2> transform)
{
    public void Fuse(IAsyncConsumator<T1> source, out IAsyncConsumator<T2> target, FlowContext context)
    {
        target = new AsyncConsumator(source, transform);
    }

    private sealed class AsyncConsumator(IAsyncConsumator<T1> source, Func<T1, T2> transform) : IAsyncConsumator<T2>
    {
        public bool TryRead([MaybeNullWhen(false)] out T2 value)
        {
            if (source.TryRead(out var original))
            {
                value = transform.Invoke(original);
                return true;
            }
            
            value = default;
            return false;
        }

        public bool IsCompleted => source.IsCompleted;

        public ValueTask<bool> WaitToReadAsync() => source.WaitToReadAsync();
    }
}