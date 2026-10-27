using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Celerity.Collections;

namespace Steelax.Pufflow.Operators.Common;

/// <summary>
///     A lightweight write-only handle over a shared <see cref="Deque{T}" /> (owned and reused by the operator):
///     an <c>Expand()</c> selector adds produced elements one by one via <see cref="Write" />, and the operator
///     pulls them out of the head as they are handed downstream. Watermark attribution is the caller's concern,
///     not the buffer's.
/// </summary>
/// <typeparam name="T">The carried value element type.</typeparam>
/// <remarks>
///     <para>
///         The struct is a copyable handle over the underlying deque (a reference type): the operator hands a
///         copy of this struct to the <c>ExpandSelector</c> for each input, additions made through the selector's
///         copy land in the operator's own buffer, and the selector must not retain the copy beyond the
///         invocation.
///     </para>
/// </remarks>
[PublicAPI]
public readonly struct QueueSpan<T>
{
    private readonly Deque<T> _queue;

    /// <summary>Wraps an operator-owned deque that produced elements are written into.</summary>
    /// <param name="queue">The shared buffer; must outlive the handle.</param>
    internal QueueSpan(Deque<T> queue) => _queue = queue;

    /// <summary>Adds one produced value to the current batch.</summary>
    /// <param name="item">The produced value.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write(T item) => _queue.PushBack(item);

    /// <summary>Removes the head value.</summary>
    /// <param name="item">The dequeued value, when one was available.</param>
    /// <returns><see langword="true" /> when a value was dequeued; otherwise <see langword="false" />.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool TryDequeue([MaybeNullWhen(false)] out T item) => _queue.TryPopFront(out item);

    /// <summary>Gets whether the buffer holds at least one produced value.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool HasAny() => _queue is { Count: > 0 };
}