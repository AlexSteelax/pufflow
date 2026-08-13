using Confluent.Kafka;
using JetBrains.Annotations;
using Steelax.Pufflow.Operators.Kafka;

namespace Steelax.Pufflow.Sdk.Test.Kafka;

/// <summary>
///     Marker exception thrown by <see cref="TestConsumer{TKey,TValue}" /> once its channel is completed
///     and drained: it signals end-of-stream so the consume loop can stop cleanly.
/// </summary>
[PublicAPI]
public sealed class TestChannelCompletedException()
    : KafkaException(new Error(ErrorCode.Unknown, "Test consumer channel completed (end of stream)."));

/// <summary>
///     The error policy for <see cref="TestConsumer{TKey,TValue}" />: treats a
///     <see cref="TestChannelCompletedException" /> as a clean end-of-stream (<see cref="KafkaErrorAction.Abort" />)
///     and delegates everything else to the <see cref="KafkaErrorPolicy.Default" /> behavior.
/// </summary>
[PublicAPI]
public sealed class TestKafkaErrorPolicy : KafkaErrorPolicy
{
    private readonly KafkaErrorPolicy _fallback = KafkaErrorPolicy.Default;

    /// <inheritdoc/>
    public override KafkaErrorAction OnConsumeError(Exception exception) =>
        exception is TestChannelCompletedException ? KafkaErrorAction.Abort : _fallback.OnConsumeError(exception);

    /// <inheritdoc/>
    public override KafkaErrorAction OnAdvanceError(Exception exception) =>
        exception is TestChannelCompletedException ? KafkaErrorAction.Abort : _fallback.OnAdvanceError(exception);
}
