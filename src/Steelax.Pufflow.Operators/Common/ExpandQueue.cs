using System.Runtime.CompilerServices;

namespace Steelax.Pufflow.Operators.Common;

/// <summary>
///     A lightweight, shared result buffer for a 1:N <c>Expand()</c> projection: wraps a self-growing
///     <see cref="Queue{T}" /> (owned and reused by the operator) and the watermark of the input the current
///     batch was produced from, so raw values can be handed out as <see cref="Carrier{T}" /> carriers.
/// </summary>
/// <typeparam name="T">The carried value element type.</typeparam>
/// <remarks>
///     <para>
///         The operator hands a copy of this struct to the <c>ExpandSelector</c> for each input; because the
///         wrapped <see cref="Queue{T}" /> is a reference type, additions made through the selector's copy land
///         in the operator's own buffer. The selector must not retain the copy beyond the invocation.
///     </para>
///     <para>
///         All values produced from a single input share its watermark, so <see cref="Begin" /> is called once
///         per input before the selector, and <see cref="TryDequeue" /> wraps every queued value with the stored
///         watermark.
///     </para>
/// </remarks>
[PublicAPI]
public struct ExpandQueue<T>
{
    private const int DefaultCapacity = 16;

    private Queue<T> _queue;
    private Watermark _watermark;

    /// <summary>Initializes a new buffer backed by a queue with the specified initial capacity.</summary>
    /// <param name="capacity">The initial capacity of the underlying queue.</param>
    internal ExpandQueue(int capacity) => _queue = new Queue<T>(capacity);

    /// <summary>
    ///     Starts a new result batch: records the watermark all values added by the following selector invocation
    ///     share.
    /// </summary>
    /// <param name="watermark">The watermark of the input element being expanded.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void Begin(Watermark watermark) => _watermark = watermark;

    /// <summary>Adds one produced value to the current batch.</summary>
    /// <param name="item">The produced value.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write(T item) => _queue.Enqueue(item);

    /// <summary>Removes the head value as a <see cref="Carrier{T}" />.</summary>
    /// <param name="carrier">The dequeued carrier, when a value was available.</param>
    /// <returns><see langword="true" /> when a value was dequeued; otherwise <see langword="false" />.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool TryDequeue(out Carrier<T> carrier)
    {
        if (_queue is { Count: > 0 } && _queue.TryDequeue(out var item))
        {
            carrier = new Carrier<T>(item, _watermark);
            return true;
        }

        carrier = default;
        return false;
    }

    /// <summary>Gets whether the buffer holds at least one produced value.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool HasAny() => _queue is { Count: > 0 };
}
