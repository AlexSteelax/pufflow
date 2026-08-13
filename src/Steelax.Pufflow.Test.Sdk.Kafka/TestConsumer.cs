using System.Runtime.InteropServices;
using System.Threading.Channels;
using Confluent.Kafka;
using JetBrains.Annotations;

namespace Steelax.Pufflow.Sdk.Test.Kafka;

public class TestConsumer<TKey, TValue> : IConsumer<TKey, TValue>
{
    private readonly Channel<ConsumeResult<TKey, TValue>> _channel;
    private readonly Dictionary<TopicPartition, Offset> _prepared;
    private readonly Dictionary<TopicPartition, Offset> _committed;

    [PublicAPI]
    public ChannelWriter<KeyValuePair<TKey, TValue>> Writer { get; }

    public TestConsumer(TimeProvider? timeProvider)
    {
        _channel = Channel.CreateUnbounded<ConsumeResult<TKey, TValue>>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = true
        });
        _prepared = new Dictionary<TopicPartition, Offset>();
        _committed = new Dictionary<TopicPartition, Offset>();
        
        Writer = new InternalChannelWriter(_channel.Writer, timeProvider);
    }

    /// <inheritdoc cref="IConsumer{TKey, TValue}.Consume(int)"/>
    public virtual ConsumeResult<TKey, TValue>? Consume(int millisecondsTimeout)
    {
        // Active polling
        if (millisecondsTimeout > 0)
            Thread.Sleep(millisecondsTimeout);
        
        return TryReadOrThrow();
    }
    
    /// <inheritdoc cref="IConsumer{TKey, TValue}.Consume(System.Threading.CancellationToken)"/>
    public virtual ConsumeResult<TKey, TValue> Consume(CancellationToken cancellationToken = default)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            if (TryReadOrThrow() is { } item)
                return item;

            Thread.Yield();
        }
        
        throw new OperationCanceledException(cancellationToken);
    }
    
    /// <inheritdoc cref="IConsumer{TKey, TValue}.Consume(System.TimeSpan)"/>
    public virtual ConsumeResult<TKey, TValue>? Consume(TimeSpan timeout)
    {
        // Active polling
        if (timeout > TimeSpan.Zero)
            Thread.Sleep(timeout);
        
        return TryReadOrThrow();
    }

    /// <summary>
    ///     Reads the next record, or reports end-of-stream: once the channel is completed and drained, a
    ///     <see cref="TestChannelCompletedException" /> is thrown so the consume loop can stop cleanly.
    /// </summary>
    private ConsumeResult<TKey, TValue>? TryReadOrThrow()
    {
        if (_channel.Reader.TryRead(out var item))
            return item;

        if (_channel.Reader.Completion.IsCompleted)
            throw new TestChannelCompletedException();

        return null;
    }
    
    /// <inheritdoc cref="IConsumer{TKey, TValue}.StoreOffset(ConsumeResult{TKey, TValue})"/>
    public virtual void StoreOffset(ConsumeResult<TKey, TValue> result) => StoreOffset(result.TopicPartitionOffset);

    /// <inheritdoc cref="IConsumer{TKey, TValue}.StoreOffset(TopicPartitionOffset)"/>
    public virtual void StoreOffset(TopicPartitionOffset offset) => OffsetSave(_prepared, offset.TopicPartition, offset.Offset);

    /// <inheritdoc cref="IConsumer{TKey, TValue}.Commit()"/>
    public virtual List<TopicPartitionOffset> Commit()
    {
        var offsets = _prepared.ToArray();
        
        foreach(var offset in offsets)
            OffsetSave(_committed, offset.Key, offset.Value);

        return _committed
            .Select(static v => new TopicPartitionOffset(v.Key, v.Value))
            .ToList();
    }

    /// <inheritdoc cref="IConsumer{TKey, TValue}.Commit(System.Collections.Generic.IEnumerable{TopicPartitionOffset})"/>
    public virtual void Commit(IEnumerable<TopicPartitionOffset> offsets)
    {
        foreach(var offset in offsets)
            OffsetSave(_committed, offset.TopicPartition, offset.Offset);
    }

    /// <inheritdoc cref="IConsumer{TKey, TValue}.Commit(ConsumeResult{TKey, TValue})"/>
    public virtual void Commit(ConsumeResult<TKey, TValue> result)
    {
        Commit([result.TopicPartitionOffset]);
    }

    /// <inheritdoc cref="IConsumer{TKey, TValue}.Close"/>
    public virtual void Close()
    {
        _channel.Writer.TryComplete();
    }

    private static void OffsetSave(Dictionary<TopicPartition, Offset> offsets, TopicPartition topicPartition, Offset offset)
    {
        ref var val = ref CollectionsMarshal.GetValueRefOrAddDefault(offsets, topicPartition, out var exists);

        if (!exists)
        {
            val = offset;
            return;
        }
        
        if (val < offset)
            val = offset;
    }

    #region NotSupported
    
    /// <inheritdoc cref="IConsumer{TKey, TValue}.Pause"/>
    public virtual void Pause(IEnumerable<TopicPartition> partitions) => throw new NotSupportedException();

    /// <inheritdoc cref="IConsumer{TKey, TValue}.Resume"/>
    public virtual void Resume(IEnumerable<TopicPartition> partitions) => throw new NotSupportedException();
    
    /// <inheritdoc cref="IConsumer{TKey, TValue}.Handle"/>
    public virtual Handle Handle => throw new NotSupportedException();
    
    /// <inheritdoc cref="IConsumer{TKey, TValue}.SetSaslCredentials"/>
    public virtual void SetSaslCredentials(string username, string password) => throw new NotSupportedException();
    
    /// <inheritdoc cref="IConsumer{TKey, TValue}.AddBrokers"/>
    public virtual int AddBrokers(string brokers) => throw new NotSupportedException();
    
    /// <inheritdoc cref="IConsumer{TKey, TValue}.Subscribe(System.Collections.Generic.IEnumerable{string})"/>
    public virtual void Subscribe(IEnumerable<string> topics) => throw new NotSupportedException();
    
    /// <inheritdoc cref="IConsumer{TKey, TValue}.Subscribe(string)"/>
    public virtual void Subscribe(string topic) => throw new NotSupportedException();
    
    /// <inheritdoc cref="IConsumer{TKey, TValue}.Unsubscribe"/>
    public virtual void Unsubscribe() => throw new NotSupportedException();
    
    /// <inheritdoc cref="IConsumer{TKey, TValue}.Assign(TopicPartition)"/>
    public virtual void Assign(TopicPartition partition) => throw new NotSupportedException();

    /// <inheritdoc cref="IConsumer{TKey, TValue}.Assign(TopicPartitionOffset)"/>
    public virtual void Assign(TopicPartitionOffset partition) => throw new NotSupportedException();

    /// <inheritdoc cref="IConsumer{TKey, TValue}.Assign(System.Collections.Generic.IEnumerable{TopicPartitionOffset})"/>
    public virtual void Assign(IEnumerable<TopicPartitionOffset> partitions) => throw new NotSupportedException();

    /// <inheritdoc cref="IConsumer{TKey, TValue}.Assign(System.Collections.Generic.IEnumerable{TopicPartition})"/>
    public virtual void Assign(IEnumerable<TopicPartition> partitions) => throw new NotSupportedException();
    
    /// <inheritdoc cref="IConsumer{TKey, TValue}.IncrementalAssign(System.Collections.Generic.IEnumerable{TopicPartitionOffset})"/>
    public virtual void IncrementalAssign(IEnumerable<TopicPartitionOffset> partitions) => throw new NotSupportedException();

    /// <inheritdoc cref="IConsumer{TKey, TValue}.IncrementalAssign(System.Collections.Generic.IEnumerable{TopicPartition})"/>
    public virtual void IncrementalAssign(IEnumerable<TopicPartition> partitions) => throw new NotSupportedException();

    /// <inheritdoc cref="IConsumer{TKey, TValue}.IncrementalUnassign"/>
    public virtual void IncrementalUnassign(IEnumerable<TopicPartition> partitions) => throw new NotSupportedException();

    /// <inheritdoc cref="IConsumer{TKey, TValue}.Unassign"/>
    public virtual void Unassign() => throw new NotSupportedException();
    
    /// <inheritdoc cref="IConsumer{TKey, TValue}.Seek"/>
    public virtual void Seek(TopicPartitionOffset tpo) => throw new NotSupportedException();
    
    /// <inheritdoc cref="IConsumer{TKey, TValue}.OffsetsForTimes"/>
    public virtual List<TopicPartitionOffset> OffsetsForTimes(IEnumerable<TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout) => throw new NotSupportedException();

    /// <inheritdoc cref="IConsumer{TKey, TValue}.GetWatermarkOffsets"/>
    public virtual WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition) => throw new NotSupportedException();

    /// <inheritdoc cref="IConsumer{TKey, TValue}.QueryWatermarkOffsets"/>
    public virtual WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout) => throw new NotSupportedException();
    
    /// <inheritdoc cref="IConsumer{TKey, TValue}.Committed(System.TimeSpan)"/>
    public virtual List<TopicPartitionOffset> Committed(TimeSpan timeout) => throw new NotSupportedException();

    /// <inheritdoc cref="IConsumer{TKey, TValue}.Committed(System.Collections.Generic.IEnumerable{TopicPartition}, System.TimeSpan)"/>
    public virtual List<TopicPartitionOffset> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout) => throw new NotSupportedException();

    /// <inheritdoc cref="IConsumer{TKey, TValue}.Position"/>
    public virtual Offset Position(TopicPartition partition) => throw new NotSupportedException();
    
    /// <inheritdoc cref="IConsumer{TKey, TValue}.MemberId"/>
    public virtual string MemberId => throw new NotSupportedException();
    
    /// <inheritdoc cref="IConsumer{TKey, TValue}.Assignment"/>
    public virtual List<TopicPartition> Assignment => throw new NotSupportedException();
    
    /// <inheritdoc cref="IConsumer{TKey, TValue}.Subscription"/>
    public virtual List<string> Subscription => throw new NotSupportedException();
    
    /// <inheritdoc cref="IConsumer{TKey, TValue}.ConsumerGroupMetadata"/>
    public virtual IConsumerGroupMetadata ConsumerGroupMetadata => throw new NotSupportedException();
    
    /// <inheritdoc cref="IConsumer{TKey, TValue}.Name"/>
    public virtual string Name => throw new NotSupportedException();
    
    #endregion NotSupported

    protected virtual void Dispose(bool _) { }

    /// <inheritdoc cref="IConsumer{TKey, TValue}.Dispose"/>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    private sealed class InternalChannelWriter(ChannelWriter<ConsumeResult<TKey, TValue>> target, TimeProvider? timeProvider) : ChannelWriter<KeyValuePair<TKey, TValue>>
    {
        private readonly TimeProvider _timeProvider = timeProvider ?? TimeProvider.System;
        private long _offset = -1;
        
        public override bool TryWrite(KeyValuePair<TKey, TValue> item)
        {
            var result = new ConsumeResult<TKey, TValue>
            {
                Message = new Message<TKey, TValue>
                {
                    Key = item.Key,
                    Value = item.Value,
                    Timestamp = new Timestamp(_timeProvider.GetUtcNow())
                },
                Topic = "default",
                Partition = 0,
                Offset = Interlocked.Increment(ref _offset),
            };
            
            return target.TryWrite(result);
        }

        public override ValueTask<bool> WaitToWriteAsync(CancellationToken cancellationToken = default) => target.WaitToWriteAsync(cancellationToken);

        public override bool TryComplete(Exception? error = null) => target.TryComplete(error);
    }
}