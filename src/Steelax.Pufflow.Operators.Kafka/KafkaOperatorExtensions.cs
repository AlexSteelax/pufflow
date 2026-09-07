using Confluent.Kafka;
using Steelax.Pufflow.Abstractions;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Kafka;

/// <summary>
///     Extension methods that attach Kafka-based stages to a dataflow source.
/// </summary>
[PublicAPI]
public static class KafkaOperatorExtensions
{
    /// <summary>
    ///     Attaches a Kafka consumer source to the flow: emits <see cref="Carrier{T}" /> elements carrying either a
    ///     consumed <see cref="ConsumeResult{TKey,TValue}" /> (when a record is available) or no payload — only a
    ///     bare progress watermark — when consumption has been quiet.
    /// </summary>
    /// <typeparam name="TKey">The Kafka message key type.</typeparam>
    /// <typeparam name="TValue">The Kafka message value type.</typeparam>
    /// <param name="flowSource">The flow source being extended.</param>
    /// <param name="consumer">The Kafka consumer; after being passed, external access is forbidden.</param>
    /// <param name="options">Consumer processor settings (window pool, intervals, advance strategy, ...).</param>
    /// <param name="commiter">
    ///     Receives the source's watermark committer: the reader publishes its progress via
    ///     <see cref="IWatermarkCommiter.SetReaderWatermark" /> to allow committed offset flushing.
    /// </param>
    /// <param name="errorPolicy">The error policy; defaults to <see cref="KafkaErrorPolicy.Default" />.</param>
    /// <param name="watermarkProvider">The watermark source; defaults to monotonic time.</param>
    /// <param name="timeProvider">The time source for timers; defaults to the system one.</param>
    /// <returns>
    ///     A source emitting <see cref="Carrier{T}" /> elements whose payload is a
    ///     <see cref="ConsumeResult{TKey,TValue}" /> when data is available, or empty (bare progress) otherwise.
    /// </returns>
    public static Source<IProducator<Carrier<ConsumeResult<TKey, TValue>>>> OnKafkaSource<TKey, TValue>(
        this FlowSource flowSource,
        IConsumer<TKey, TValue> consumer,
        KafkaConsumerOptions options,
        out IWatermarkCommiter commiter,
        KafkaErrorPolicy? errorPolicy = null,
        WatermarkProvider? watermarkProvider = null,
        TimeProvider? timeProvider = null)
    {
        var processor = new KafkaConsumerProcessor<TKey, TValue>(consumer, options, errorPolicy, watermarkProvider, timeProvider);
        commiter = processor;
        return flowSource.On(processor);
    }
}