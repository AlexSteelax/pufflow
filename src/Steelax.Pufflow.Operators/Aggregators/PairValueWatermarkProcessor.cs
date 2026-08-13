using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Aggregators;

[Flow]
internal sealed partial class PairValueWatermarkProcessor<T> : IAsyncProducator<Unio<T, Watermark>>
{
    private IAsyncProducator<Watermarked<T>> _target = null!;
    private PendingValue<T> _pending;
    private Watermark _watermark = Watermark.Nothing();

    public void Fuse(out IAsyncProducator<Unio<T, Watermark>> source, IAsyncProducator<Watermarked<T>> target, FlowContext context)
    {
        _target = target;
        source = this;
    }

    public bool TryWrite(Unio<T, Watermark> item)
    {
        if (item.TryPickT0(out var value, out var watermark))
        {
            if (_pending.Occupied)
            {
                if (_target.TryWrite(new Watermarked<T>(_pending.Value, _watermark)))
                {
                    _pending = new PendingValue<T>(value);
                    _watermark = Watermark.Nothing();
                    return true;
                }

                return false;
            }
            
            _pending = new PendingValue<T>(value);
            return true;
        }
        else
        {
            if (_pending.Occupied)
            {
                if (_target.TryWrite(new Watermarked<T>(_pending.Value, watermark)))
                {
                    _pending = default;
                    _watermark = Watermark.Nothing();
                    return true;
                }
                
                return false;
            }

            _watermark = watermark;
            return true;
        }
    }

    public ValueTask<bool> WaitToWriteAsync() => _target.WaitToWriteAsync();
    public bool TryComplete(Exception? ex = null) => _target.TryComplete(ex);
}