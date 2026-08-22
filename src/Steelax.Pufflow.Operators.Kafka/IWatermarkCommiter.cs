using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Kafka;

/// <summary>
///     Reports the reader's (pipeline) processing watermark back to a Kafka consumer source.
/// </summary>
/// <remarks>
///     The consumer source emits watermarked records and tracks progress per window; once the reader
///     confirms it processed up to a watermark, the source can flush (commit) the corresponding windows.
///     This interface is the one-way, thread-safe channel through which a downstream reader publishes its
///     progress. Implemented by <see cref="KafkaConsumerProcessor{TKey,TValue}" />.
/// </remarks>
[PublicAPI]
public interface IWatermarkCommiter
{
    /// <summary>
    ///     Publishes the reader's watermark: the mark up to which the pipeline has processed records.
    /// </summary>
    /// <param name="watermark">The watermark up to which records have been processed.</param>
    /// <remarks>
    ///     Safe to call from any thread. Monotonic progress: publishing a watermark lower than a previously
    ///     reported one does not move the source's commit point backwards.
    /// </remarks>
    void SetReaderWatermark(Watermark watermark);
}