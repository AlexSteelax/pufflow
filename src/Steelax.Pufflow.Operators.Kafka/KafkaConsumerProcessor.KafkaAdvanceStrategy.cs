using Confluent.Kafka;

namespace Steelax.Pufflow.Operators.Kafka;

internal sealed partial class KafkaConsumerProcessor<TKey, TValue>
{
    /// <summary>
    /// Advances committed offsets back to Kafka after records are processed.
    /// </summary>
    internal abstract class KafkaAdvanceStrategy
    {
        /// <summary>
        /// Advances the committed position for the given offsets.
        /// </summary>
        /// <param name="offsets">Per-partition offsets ready to be committed.</param>
        public abstract void Advance<TCollection>(TCollection offsets)
            where TCollection : IEnumerable<TopicPartitionOffset>;

        /// <summary>
        /// Stores offsets locally for auto-commit.
        /// </summary>
        private sealed class OffsetStoreStrategy(IConsumer<TKey, TValue> consumer) : KafkaAdvanceStrategy
        {
            /// <inheritdoc/>
            public override void Advance<TCollection>(TCollection offsets)
            {
                foreach (var offset in offsets)
                    consumer.StoreOffset(offset);
            }
        }

        /// <summary>
        /// Synchronously commits offsets to the broker.
        /// </summary>
        private sealed class ManualAdvanceStrategy(IConsumer<TKey, TValue> consumer) : KafkaAdvanceStrategy
        {
            /// <inheritdoc/>
            public override void Advance<TCollection>(TCollection offsets)
            {
                consumer.Commit(offsets);
            }
        }

        public static KafkaAdvanceStrategy Create(IConsumer<TKey, TValue> consumer, AdvanceStrategy strategy)
        {
            return strategy switch
            {
                AdvanceStrategy.OffsetStore => new OffsetStoreStrategy(consumer),
                AdvanceStrategy.ManualCommit => new ManualAdvanceStrategy(consumer),
                _ => throw new ArgumentOutOfRangeException(nameof(strategy), strategy, null)
            };
        }
    }
}