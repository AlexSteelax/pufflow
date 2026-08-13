namespace Steelax.Pufflow;

/// <summary>
///     Provides extension methods for chaining dataflow pipeline components.
/// </summary>
/// <remarks>
///     This partial class contains <c>Next</c> extension methods that connect
///     <see cref="Source{T}" />, <see cref="Sink{T}" />, and <see cref="Pipe{TLeft,TRight}" />
///     markers into a complete pipeline.
///     Each overload handles a specific combination of poll/push interfaces
///     (e.g., <c>IEnumerator → IEnumerator</c>, <c>IAsyncEnumerator → Sink</c>).
/// </remarks>
public static partial class FlowExt;