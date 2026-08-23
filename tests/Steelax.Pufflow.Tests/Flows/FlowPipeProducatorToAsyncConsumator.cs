using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Tests.Flows;

[Flow]
public partial class FlowPipeProducatorToAsyncConsumator<T1, T2>(Func<T1, T2> transform)
{
    public void Fuse(out IProducator<T1> source, out IAsyncConsumator<T2> target, FlowContext context)
    {
        var bridge = new Bridge(transform);
        source = bridge;
        target = bridge;
    }

    private sealed class Bridge(Func<T1, T2> transform) : IProducator<T1>, IAsyncConsumator<T2>
    {
        private readonly Channel<T2> _channel = Channel.CreateUnbounded<T2>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true,
            AllowSynchronousContinuations = true
        });

        public bool TryWrite(T1 value) => _channel.Writer.TryWrite(transform.Invoke(value));

        public bool TryComplete(Exception? ex = null) => _channel.Writer.TryComplete(ex);

        public bool TryRead([MaybeNullWhen(false)] out T2 value) => _channel.Reader.TryRead(out value);

        public bool IsCompleted => _channel.Reader.Completion.IsCompleted;

        public ValueTask<bool> WaitToReadAsync() => _channel.Reader.WaitToReadAsync();
    }
}
