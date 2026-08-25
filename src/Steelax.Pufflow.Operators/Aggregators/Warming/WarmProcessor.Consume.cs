using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Aggregators.Warming;

/// <summary>
///     Input handling for the <see cref="WarmProcessor{TKey,TValue,TGroup,TWarm}" />: reads watermarked
///     values from the supplied <see cref="IAsyncConsumator{T}" /> without a buffer, retaining the current
///     value in a single pending slot when it cannot be processed yet. Readiness is observed through
///     <see cref="WarmProcessor{TKey,TValue,TGroup,TWarm}._input" /> so the loop sleeps on the fan-in
///     instead of polling.
/// </summary>
internal sealed partial class WarmProcessor<TKey, TValue, TGroup, TWarm>
{
    /// <summary>The value currently read from the input and held until it is fully handled.</summary>
    private PendingConsume _pendingInput;

    private bool _completedInput;

    private bool TryPeekSource<TReader>(TReader reader, out Watermarked<TValue> item)
        where TReader : IAsyncConsumator<Watermarked<TValue>>
    {
        if (_pendingInput.Occupied)
        {
            item = _pendingInput.Value;
            return true;
        }

        if (reader.TryRead(out item))
        {
            _pendingInput = new PendingConsume(item, true);
            return true;
        }

        if (reader.IsCompleted)
        {
            _completedInput = true;
        }
        else
        {
            // The source has no data yet but is not done: arm the readiness observation so the loop
            // wakes through the input slot when a value arrives or the stream completes. A fresh
            // observation is armed only when the previous one has already resolved — the EventTask
            // rejects a new task while one is still in flight.
            if (!_input.GetState().IsPending)
                _input.Observe(reader.WaitToReadAsync());
        }

        _pendingInput = default;
        return false;
    }

    private void AdvanceSource<TReader>(TReader reader)
        where TReader : IAsyncConsumator<Watermarked<TValue>>
    {
        if (_pendingInput.Occupied)
        {
            _pendingInput = default;
            return;
        }

        _input.Observe(reader.WaitToReadAsync());
    }

    private bool IsCompletedSource => _completedInput;
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="value"></param>
    /// <param name="occupied"></param>
    internal struct PendingConsume(Watermarked<TValue> value, bool occupied)
    {
        /// <summary>
        /// 
        /// </summary>
        public readonly bool Occupied = occupied;

        /// <summary>
        /// 
        /// </summary>
        public readonly Watermarked<TValue> Value = value;
    }
}
