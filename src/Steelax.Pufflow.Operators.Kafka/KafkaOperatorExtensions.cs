using Confluent.Kafka;
using Steelax.Pufflow.Abstractions;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Kafka;

/// <summary>
/// 
/// </summary>
public static class KafkaOperatorExtensions
{
    /// <summary>
    /// 
    /// </summary>
    /// <param name="flowSource"></param>
    /// <param name="consumer"></param>
    /// <param name="options"></param>
    /// <param name="errorPolicy"></param>
    /// <param name="watermarkProvider"></param>
    /// <param name="timeProvider"></param>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    /// <returns></returns>
    public static Source<IProducator<Watermarked<ConsumeResult<TKey, TValue>>>> OnKafkaSource<TKey, TValue>(
        this FlowSource flowSource,
        IConsumer<TKey, TValue> consumer,
        KafkaConsumerOptions options,
        KafkaErrorPolicy? errorPolicy = null,
        WatermarkProvider? watermarkProvider = null,
        TimeProvider? timeProvider = null)
    {
        var processor = new KafkaConsumerProcessor<TKey, TValue>(consumer, options, errorPolicy, watermarkProvider, timeProvider);
        return flowSource.On(processor);
    }
}