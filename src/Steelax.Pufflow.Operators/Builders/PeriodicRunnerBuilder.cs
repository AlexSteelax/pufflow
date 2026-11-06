#pragma warning disable CS1591
using Steelax.Pufflow.Operators.Aggregators;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Builders;

/// <summary>
///     Fluent chain of the periodic-runner stage started by <c>PeriodicRunner(handler)</c> and finished by
///     <see cref="Every" />.
/// </summary>
/// <typeparam name="T">The carried value element type.</typeparam>
public readonly ref struct PeriodicRunnerBuilder<T>
{
    private readonly Fuse<IAsyncConsumator<Carrier<T>>> _left;
    private readonly Action _handler;

    internal PeriodicRunnerBuilder(Fuse<IAsyncConsumator<Carrier<T>>> left, Action handler)
    {
        _left = left;
        _handler = handler;
    }

    public Fuse<IAsyncConsumator<Carrier<T>>> Every(TimeSpan period, TimeProvider? timeProvider = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(period, TimeSpan.Zero);

        return _left.Next(PeriodicRunnerProcessor<T>.Configure(_handler, period, timeProvider));
    }
}