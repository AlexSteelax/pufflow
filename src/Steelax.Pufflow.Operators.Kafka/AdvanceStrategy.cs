namespace Steelax.Pufflow.Operators.Kafka;

/// <summary>
/// Defines how consumer offsets are committed to Kafka.
/// </summary>
[PublicAPI]
public enum AdvanceStrategy
{
    /// <summary>
    /// Stores offsets locally; the auto-commit background thread flushes them.
    /// </summary>
    OffsetStore,
    
    /// <summary>
    /// Synchronously commits offsets to the broker.
    /// </summary>
    ManualCommit
}
