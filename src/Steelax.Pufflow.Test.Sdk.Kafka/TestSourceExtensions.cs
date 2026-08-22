using System.Threading.Channels;
using Confluent.Kafka;
using JetBrains.Annotations;
using Steelax.Pufflow.Abstractions;
using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Operators.Kafka;

namespace Steelax.Pufflow.Sdk.Test.Kafka;

[PublicAPI]
public static class TestSourceExtensions
{
    private static void InitialFill<T>(ChannelWriter<T> writer, IEnumerable<T>? initial)
    {
        if (initial is null)
            return;

        if (initial.Any(item => !writer.TryWrite(item)))
            throw new InvalidOperationException();
    }
    
    extension(FlowSource flowSource)
    {
        public Source<IProducator<Watermarked<ConsumeResult<TKey, TValue>>>> OnKafkaSource<TKey, TValue>(
            KafkaConsumerOptions options,
            out ChannelWriter<KeyValuePair<TKey, TValue>> writer,
            out IWatermarkCommiter commiter,
            IEnumerable<KeyValuePair<TKey, TValue>>? initial = null,
            WatermarkProvider? watermarkProvider = null,
            TimeProvider? timeProvider = null)
        {
            var consumer = new TestConsumer<TKey, TValue>(timeProvider);
            
            InitialFill(consumer.Writer, initial);
            
            writer = consumer.Writer;
            
            return flowSource.OnKafkaSource(
                consumer,
                options,
                out commiter,
                new TestKafkaErrorPolicy(),
                watermarkProvider,
                timeProvider);
        }
    }
    
    extension(FlowSource flowSource)
    {
        public Source<IProducator<Watermarked<ConsumeResult<TKey, TValue>>>> OnKafkaSource<TKey, TValue>(
            KafkaConsumerOptions options,
            IEnumerable<KeyValuePair<TKey, TValue>> items,
            out IWatermarkCommiter commiter,
            WatermarkProvider? watermarkProvider = null,
            TimeProvider? timeProvider = null)
        {
            ChannelWriter<KeyValuePair<TKey, TValue>>? writer = null;
            try
            {
                return flowSource.OnKafkaSource(options, out writer, out commiter, items, watermarkProvider, timeProvider);
            }
            finally
            {
                writer?.TryComplete();
            }
        }
    }
}