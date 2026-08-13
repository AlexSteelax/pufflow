namespace Steelax.Pufflow.Operators.Kafka;

/// <summary>
///     Internal flow-control exception: thrown when a <see cref="KafkaErrorPolicy" /> decides to abort the
///     consume loop cleanly (<see cref="KafkaErrorAction.Abort" />). Caught only by the consume loop itself
///     and never observed by the caller — it is not an error condition.
/// </summary>
internal sealed class KafkaLoopAbortException : Exception;
