namespace Steelax.Pufflow;

/// <summary>
///     Thrown when a flow node cannot be built or connected: no supported handler is found on a node, or
///     two nodes cannot be merged (incompatible kinds or a type mismatch on the joint).
/// </summary>
[PublicAPI]
public sealed class FlowMetaException : Exception
{
    /// <summary>Initializes a new instance with the given message.</summary>
    /// <param name="message">The error message.</param>
    public FlowMetaException(string message) : base(message)
    {
    }

    /// <summary>Initializes a new instance with the given message and inner exception.</summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The exception that caused this one.</param>
    public FlowMetaException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
