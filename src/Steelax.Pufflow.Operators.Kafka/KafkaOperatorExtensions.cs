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
    ///     Starts the fluent Kafka consumer source builder: carries the processor settings
    ///     (<see cref="KafkaConsumerOptions" />) and finishes with
    ///     <see cref="KafkaConsumerBuilder.Use{TKey,TValue}" /> which binds the consumer and the tracker.
    ///     The built source emits <see cref="Carrier{T}" /> elements carrying either a consumed
    ///     <see cref="ConsumeResult{TKey,TValue}" /> (when a record is available) or no payload — only a bare
    ///     progress watermark — when consumption has been quiet.
    /// </summary>
    /// <param name="flowSource">The flow source hosting the consumer stage.</param>
    /// <param name="options">Consumer processor settings (window pool, intervals, advance strategy, ...).</param>
    /// <returns>
    ///     A chain entry point: continue with <c>Use(consumer, tracker)</c>, optionally preceded by
    ///     <c>WithErrorPolicy</c> and <c>WithCustomProviders</c>.
    /// </returns>
    [PublicAPI]
    public static KafkaConsumerBuilder OnKafkaSource(this FlowSource flowSource, KafkaConsumerOptions options) =>
        new(flowSource, options);
}
