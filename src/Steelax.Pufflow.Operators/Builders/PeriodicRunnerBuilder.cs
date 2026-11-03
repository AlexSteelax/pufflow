using Steelax.Pufflow.Operators.Aggregators;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Builders;

/// <summary>
///     Fluent chain of the periodic-runner stage started by <c>PeriodicRunner(handler)</c> and finished by
///     <see cref="Every" />. The runner consumes the incoming <see cref="Carrier{T}" /> stream, strips
///     watermarks from data carriers and holds back the latest meaningful watermark; on every interval tick it
///     invokes the handler supplied at the entry point and, after it returns, releases the held watermark
///     downstream as a bare-progress carrier (a natural heartbeat).
/// </summary>
/// <typeparam name="T">The carried value element type.</typeparam>
/// <remarks>
///     This is a <c>ref struct</c>: it is intended as a local chain temporary and must not be stored in
///     fields, collections or captured by closures.
/// </remarks>
public readonly ref struct PeriodicRunnerBuilder<T>
{
    private readonly Source<IAsyncConsumator<Carrier<T>>> _left;
    private readonly Action _handler;

    internal PeriodicRunnerBuilder(Source<IAsyncConsumator<Carrier<T>>> left, Action handler)
    {
        _left = left;
        _handler = handler;
    }

    /// <summary>
    ///     Finishes the chain with the given interval, wiring a <c>PeriodicRunnerProcessor</c> into the source:
    ///     values pass through with their watermarks stripped, the handler runs once per interval, and the
    ///     aggregated watermark is pushed downstream after each handler invocation (and once more when the
    ///     source completes).
    /// </summary>
    /// <param name="period">The interval between handler invocations; must be positive.</param>
    /// <param name="timeProvider">
    ///     The time source for the interval; <see langword="null" /> defaults to the system one.
    /// </param>
    /// <returns>The carried stream with the periodically released progress.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="period" /> is not positive.</exception>
    public Source<IAsyncConsumator<Carrier<T>>> Every(TimeSpan period, TimeProvider? timeProvider = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(period, TimeSpan.Zero);

        var processor = new PeriodicRunnerProcessor<T>(_handler, period, timeProvider);
        return _left.Next(processor.FlowAConsToACons);
    }
}