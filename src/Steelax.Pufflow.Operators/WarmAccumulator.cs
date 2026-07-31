using System.Diagnostics.CodeAnalysis;

namespace Steelax.Pufflow.Operators;

/// <summary>
/// Per-key accumulator in the warming pipeline: a weighted delayed buffer whose consumption is
/// iterative and resumable — a group is fetched and cached by <see cref="TryPeek"/> and only
/// accepted via <see cref="AdvanceOrComplete"/> once it has been handed downstream successfully.
/// </summary>
/// <typeparam name="TValue">The input value type from the source stream.</typeparam>
/// <typeparam name="TGroup">The output group type produced when the key is consumed.</typeparam>
/// <remarks>
/// <para>
/// Every added value contributes a <c>weight</c> — its significance/size. The accumulator tracks
/// its own held weight (<see cref="_internalWeight"/>); the processor keeps a single global budget
/// shared across all accumulators to bound the delayed buffer.
/// </para>
/// <para>
/// The subclass maintains <see cref="EstimatedWeight"/> — the weight the next <see cref="InternalAdd"/>
/// is expected to contribute (zero when a subsequent value would not add weight, e.g. it fills an
/// already-counted batch). The processor uses it to enforce the budget <em>before</em> accumulating.
/// <see cref="InternalAdd"/> charges that estimate, then stores the value via <see cref="Add"/>.
/// </para>
/// <para>
/// Draining is iterative and resumable: a group is peeked (fetched and cached) by <see cref="TryPeek"/>
/// without being consumed, so if the output queue is full the group stays pending and the drain can be
/// retried later; once the group is written successfully, <see cref="AdvanceOrComplete"/> accepts it
/// and releases its weight. When the accumulator is exhausted, <see cref="AdvanceOrComplete"/> completes
/// it and releases all remaining held weight.
/// </para>
/// </remarks>
[PublicAPI]
public abstract class WarmAccumulator<TValue, TGroup>
{
    private long _internalWeight;

    private Peeked _peeked;

    /// <summary>
    /// The weight the next <see cref="InternalAdd"/> is expected to contribute to the budget. Must be
    /// initialized by the subclass (for the first value) and updated by <see cref="Add"/> after each
    /// stored value; zero means the next value adds no weight.
    /// </summary>
    protected internal int EstimatedWeight { get; }

    /// <summary>Stores a value and updates <see cref="EstimatedWeight"/> for the next value.</summary>
    /// <param name="value">The value to store.</param>
    protected abstract void Add(TValue value);

    /// <summary>
    /// Fetches the next pending group and reports the weight it holds. Called by the base class to
    /// fill the peek cache; the group is not considered consumed until <see cref="AdvanceOrComplete"/>.
    /// </summary>
    /// <param name="group">The next group, if any.</param>
    /// <param name="weight">The weight held by <paramref name="group"/>.</param>
    /// <returns><see langword="true"/> if a group was fetched; otherwise, <see langword="false"/> (exhausted).</returns>
    protected abstract bool TryConsume(out TGroup group, out int weight);

    /// <summary>Adds a value, charging this accumulator's held weight; returns the weight to charge the global budget.</summary>
    /// <param name="value">The value to store.</param>
    /// <returns>The weight contributed by <paramref name="value"/> (already charged to the held weight).</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <see cref="EstimatedWeight"/> is negative.</exception>
    internal int InternalAdd(TValue value)
    {
        var weight = EstimatedWeight;
        ArgumentOutOfRangeException.ThrowIfNegative(weight);
        _internalWeight += weight;
        Add(value);
        return weight;
    }

    /// <summary>Peeks the next pending group without advancing; the peeked group is cached until <see cref="AdvanceOrComplete"/>.</summary>
    /// <param name="group">The pending group, if any.</param>
    /// <returns><see langword="true"/> if a group is pending; otherwise, <see langword="false"/> (exhausted).</returns>
    internal bool TryPeek([MaybeNullWhen(false)] out TGroup group)
    {
        switch (_peeked.Stage)
        {
            case PeekedStage.Default:
                if (TryConsume(out group, out var weight))
                {
                    _peeked = new Peeked(group, weight, PeekedStage.Ready);
                    return true;
                }

                // Exhausted — stay completed so subsequent peeks do not re-fetch.
                _peeked = new Peeked(default!, 0, PeekedStage.Completed);
                group = default!;
                return false;

            case PeekedStage.Ready:
                group = _peeked.Value;
                return true;

            case PeekedStage.Completed:
                group = default!;
                return false;

            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    /// <summary>
    /// Accepts the peeked group (releasing its weight from the held budget) or, when the accumulator
    /// is exhausted, completes it (releasing all remaining held weight). Returns the weight the caller
    /// removes from the global budget.
    /// </summary>
    /// <returns>The released weight.</returns>
    internal int AdvanceOrComplete()
    {
        if (_peeked.Stage == PeekedStage.Ready)
        {
            // Accept the cached group: release exactly what it holds (clamped to the held weight).
            var released = Math.Min(_internalWeight, _peeked.Weight);
            _internalWeight -= released;
            _peeked = default;
            return (int)released;
        }

        if (_peeked.Stage == PeekedStage.Completed)
        {
            // Exhausted: release the remaining held weight back to the budget.
            var released = _internalWeight;
            _internalWeight = 0;
            return (int)released;
        }

        // Nothing was peeked — nothing to release.
        return 0;
    }

    private readonly record struct Peeked(TGroup Value, int Weight, PeekedStage Stage);

    private enum PeekedStage : byte
    {
        Default,
        Ready,
        Completed
    }
}
