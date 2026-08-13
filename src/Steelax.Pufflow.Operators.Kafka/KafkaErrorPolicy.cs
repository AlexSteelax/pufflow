using Confluent.Kafka;

namespace Steelax.Pufflow.Operators.Kafka;

/// <summary>
///     The action the consume loop takes after an error was reported by a <see cref="KafkaErrorPolicy" />.
/// </summary>
[PublicAPI]
public enum KafkaErrorAction
{
    /// <summary>
    ///     Suppress the error and keep the loop running: the poll is treated as empty (consume) or the
    ///     commit is skipped and retried on the next watermark cutoff (advance).
    /// </summary>
    Continue,

    /// <summary>
    ///     Stop the loop cleanly without faulting the pipeline (e.g. end-of-stream). The output is
    ///     completed normally and the consumer is closed.
    /// </summary>
    Abort,

    /// <summary>
    ///     Rethrow the error and fault the pipeline.
    /// </summary>
    Throw
}

/// <summary>
///     Decides how the Kafka consume loop reacts to errors in the two places where they can occur:
///     consuming (<c>consumer.Consume</c>) and advancing offsets (<c>Commit</c>/<c>StoreOffset</c>).
/// </summary>
/// <remarks>
///     Each handler receives the thrown exception and returns a <see cref="KafkaErrorAction" />: suppress
///     and continue the loop, abort it cleanly, or rethrow and fault the pipeline. The <see cref="Default" />
///     policy suppresses transient errors (timeout / transport / rebalance) and faults on fatal ones
///     (unknown topic / authorization).
/// </remarks>
[PublicAPI]
public abstract class KafkaErrorPolicy
{
    /// <summary>
    ///     Handles an error raised while consuming (<c>consumer.Consume</c>).
    /// </summary>
    /// <remarks>
    ///     Returning <see cref="KafkaErrorAction.Continue" /> treats the error as an empty poll (the loop
    ///     falls back to idle and retries); <see cref="KafkaErrorAction.Abort" /> stops the loop cleanly;
    ///     <see cref="KafkaErrorAction.Throw" /> rethrows and faults the pipeline.
    /// </remarks>
    [PublicAPI]
    public abstract KafkaErrorAction OnConsumeError(Exception exception);

    /// <summary>
    ///     Handles an error raised while advancing offsets (<c>Commit</c>/<c>StoreOffset</c>).
    /// </summary>
    /// <remarks>
    ///     Returning <see cref="KafkaErrorAction.Continue" /> skips the current commit: the window stays
    ///     closed and is retried on the next watermark cutoff; <see cref="KafkaErrorAction.Abort" /> stops
    ///     the loop cleanly; <see cref="KafkaErrorAction.Throw" /> rethrows and faults the pipeline.
    /// </remarks>
    [PublicAPI]
    public abstract KafkaErrorAction OnAdvanceError(Exception exception);

    /// <summary>
    ///     The default policy: transient consume errors are suppressed (continue), fatal ones fault the
    ///     pipeline; advance (commit) errors are always suppressed because a skipped commit is retried later.
    /// </summary>
    [PublicAPI]
    public static KafkaErrorPolicy Default { get; } = new KafkaErrorDefaultPolicy();

    /// <summary>
    ///     The strict policy: every consume and advance (commit) error faults the pipeline. No error is
    ///     suppressed — fail fast rather than silently skip data or a commit.
    /// </summary>
    [PublicAPI]
    public static KafkaErrorPolicy Strict { get; } = new KafkaErrorStrictPolicy();

    /// <summary>
    ///     Creates a policy from two delegates: one for consume errors and one for advance (commit) errors.
    /// </summary>
    /// <param name="onConsumeError">Decides the action for a consume error.</param>
    /// <param name="onAdvanceError">Decides the action for an advance (commit) error.</param>
    /// <returns>The custom policy.</returns>
    [PublicAPI]
    public static KafkaErrorPolicy CreateCustom(
        Func<Exception, KafkaErrorAction> onConsumeError,
        Func<Exception, KafkaErrorAction> onAdvanceError)
    {
        return new KafkaErrorCustomPolicy(onConsumeError, onAdvanceError);
    }

    private sealed class KafkaErrorCustomPolicy(
        Func<Exception, KafkaErrorAction> onConsumeError,
        Func<Exception, KafkaErrorAction> onAdvanceError) : KafkaErrorPolicy
    {
        public override KafkaErrorAction OnConsumeError(Exception exception) => onConsumeError.Invoke(exception);
        public override KafkaErrorAction OnAdvanceError(Exception exception) => onAdvanceError.Invoke(exception);
    }

    private sealed class KafkaErrorDefaultPolicy : KafkaErrorPolicy
    {
        public override KafkaErrorAction OnConsumeError(Exception exception) => exception switch
        {
            KafkaException
            {
                Error.Code:
                ErrorCode.Local_TimedOut or
                ErrorCode.Local_Transport or
                ErrorCode.RebalanceInProgress or
                ErrorCode.NotCoordinatorForGroup or
                ErrorCode.GroupCoordinatorNotAvailable
            } => KafkaErrorAction.Continue,
            _ => KafkaErrorAction.Throw
        };

        public override KafkaErrorAction OnAdvanceError(Exception exception) => KafkaErrorAction.Continue;
    }

    private sealed class KafkaErrorStrictPolicy : KafkaErrorPolicy
    {
        public override KafkaErrorAction OnConsumeError(Exception exception) => KafkaErrorAction.Throw;

        public override KafkaErrorAction OnAdvanceError(Exception exception) => KafkaErrorAction.Throw;
    }
}
