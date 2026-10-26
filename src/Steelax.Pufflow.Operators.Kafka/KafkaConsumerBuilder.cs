using Confluent.Kafka;
using Steelax.Pufflow.Abstractions;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Kafka;

/// <summary>
///     The accumulated configuration of the Kafka consumer source stage, carried through the builder chain.
///     The consumer and the tracker are only introduced by the terminal <c>Use</c> step, so they are not part
///     of the context. All fields are known upfront (the key and value types come from the
///     <see cref="IConsumer{TKey,TValue}" /> itself), so the builder needs no generic parameters.
/// </summary>
internal record KafkaConsumerBuilderContext(
    FlowSource FlowSource,
    KafkaConsumerOptions Options,
    KafkaErrorPolicy ErrorPolicy = null!,
    WatermarkProvider? WatermarkProvider = null,
    TimeProvider? TimeProvider = null
);

/// <summary>
///     Fluent entry point of the Kafka consumer source stage: carries the processor settings
///     (<see cref="KafkaConsumerOptions" />) around while the optional policy and provider
///     customizations are applied, and finishes with <see cref="Use{TKey,TValue}" /> which binds the
///     consumer and the tracker and builds the stage.
/// </summary>
/// <remarks>
///     This is a <c>ref struct</c>: it is intended as a local chain temporary and must not be stored in
///     fields, collections or captured by closures.
/// </remarks>
public readonly ref struct KafkaConsumerBuilder
{
    private readonly KafkaConsumerBuilderContext _context;

    internal KafkaConsumerBuilder(FlowSource flowSource, KafkaConsumerOptions options) => _context =
        new KafkaConsumerBuilderContext(flowSource, options);

    internal KafkaConsumerBuilder(KafkaConsumerBuilderContext context) => _context = context;

    /// <summary>
    ///     Sets the policy deciding how consume and advance (commit) errors are handled; defaults to
    ///     <see cref="KafkaErrorPolicy.Default" />.
    /// </summary>
    /// <param name="errorPolicy">The error policy; pass <see cref="KafkaErrorPolicy.Default" /> or a custom one.</param>
    /// <returns>The same chain step, carrying the policy.</returns>
    [PublicAPI]
    public KafkaConsumerBuilder WithErrorPolicy(KafkaErrorPolicy errorPolicy) =>
        new(_context with { ErrorPolicy = errorPolicy });

    /// <summary>
    ///     Sets the watermark source for emitted messages and the time source for the lifecycle timers.
    ///     Defaults to monotonic time (<see cref="WatermarkProvider.System" />) and the system clock;
    ///     overridable for deterministic tests.
    /// </summary>
    /// <param name="watermarkProvider">The watermark provider; <see langword="null" /> keeps the default.</param>
    /// <param name="timeProvider">The time source; <see langword="null" /> keeps the default.</param>
    /// <returns>The same chain step, carrying the providers.</returns>
    [PublicAPI]
    public KafkaConsumerBuilder WithCustomProviders(WatermarkProvider? watermarkProvider = null, TimeProvider? timeProvider = null) =>
        new(_context with { WatermarkProvider = watermarkProvider, TimeProvider = timeProvider });

    /// <summary>
    ///     Binds the consumer and the shared progress tracker and builds the
    ///     <see cref="KafkaConsumerProcessor{TKey,TValue}" />, attaching it to the owning
    ///     <see cref="FlowSource" />. The reader publishes its processing watermark via
    ///     <see cref="WatermarkTracker.SetProgress" />, and the source reads it back via
    ///     <see cref="WatermarkTracker.GetProgress" /> to flush confirmed offsets. One tracker can be shared
    ///     between several sources (e.g. the inputs of a merge).
    /// </summary>
    /// <typeparam name="TKey">The Kafka message key type.</typeparam>
    /// <typeparam name="TValue">The Kafka message value type.</typeparam>
    /// <param name="consumer">The Kafka consumer; after being passed, external access is forbidden.</param>
    /// <param name="tracker">The shared progress tracker.</param>
    /// <returns>
    ///     A source emitting <see cref="Carrier{T}" /> elements whose payload is a
    ///     <see cref="ConsumeResult{TKey,TValue}" /> when data is available, or empty (bare progress) otherwise.
    /// </returns>
    [PublicAPI]
    public Source<IProducator<Carrier<ConsumeResult<TKey, TValue>>>> Use<TKey, TValue>(
        IConsumer<TKey, TValue> consumer,
        WatermarkTracker tracker)
    {
        var processor = new KafkaConsumerProcessor<TKey, TValue>(
            consumer,
            _context.Options,
            tracker,
            _context.ErrorPolicy,
            _context.WatermarkProvider,
            _context.TimeProvider);

        return _context.FlowSource.On(processor);
    }
}
