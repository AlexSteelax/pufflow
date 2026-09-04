using System.Diagnostics.CodeAnalysis;
using Steelax.Pufflow.Operators.Abstractions;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Transforms;

/// <summary>
///     A stateless filter pipe that forwards elements of <typeparamref name="TSource" /> only when they satisfy a
///     <see cref="FilterPredicate{TSource}" />. Elements failing the predicate are dropped and never reach the
///     downstream endpoint.
/// </summary>
/// <typeparam name="TSource">The element type flowing through the pipe.</typeparam>
/// <typeparam name="TScope">
///     The type of the operator-held scope state (typically the user-provided predicate itself), which is handed to
///     the internal predicate on each invocation. Kept as a type so the projection can be a cached static method that
///     the JIT can inline on the hot path.
/// </typeparam>
/// <typeparam name="TArgs">
///     The type of the fixed per-pipe arguments the predicate receives (a <see cref="Unit" /> padding when the public
///     predicate takes no extra arguments).
/// </typeparam>
/// <remarks>
///     <para>
///         The filter is a 1:0/1 element transform: an accepted input produces exactly one output of the same type,
///         while a rejected input produces nothing. Because it never changes the element type, the same
///         <typeparamref name="TSource" /> flows on the input and the output side of every exposed endpoint.
///     </para>
///     <para>
///         The component is stateless and allocation-free on the hot path: no buffering and no background task are
///         used. Backpressure is propagated unchanged by delegating <c>IsFull</c>/<c>WaitToWriteAsync</c> and
///         <c>IsCompleted</c>/<c>WaitToReadAsync</c> to the neighbouring endpoint, and completion is proxied verbatim.
///     </para>
///     <para>
///         <b>Note on design</b> — this class only backs plain (non-watermarked) <c>Filter</c>. For a
///         <see cref="Watermarked{T}" /> input a dropped element is <i>not</i> silently removed: it is turned into a
///         bare <see cref="Unit" /> progress marker inside a <c>Unio</c> payload under the same watermark, so the
///         stream's progress state keeps flowing. Such watermarked cases reuse the 1:1 mapping processor and are not
///         handled here.
///     </para>
/// </remarks>
[Flow]
internal sealed partial class BypassFilterProcessor<TSource, TScope, TArgs>(FilterPredicate<TSource, TScope, TArgs> predicate, TScope scope, TArgs args)
{
    /// <summary>
    ///     Exposes this filter as the upstream push input processor for an <see cref="IAsyncProducator{TSource}" />
    ///     source, writing accepted elements into <paramref name="target" /> and proxying completion.
    /// </summary>
    /// <param name="source">The push side this component implements; written by the upstream source.</param>
    /// <param name="target">The downstream asynchronous push producer to write the accepted elements into.</param>
    /// <param name="context">The flow context providing cancellation for the pipeline.</param>
    public void Fuse(out IAsyncProducator<TSource> source, IAsyncProducator<TSource> target, FlowContext context)
    {
        source = new AsyncProducator(target, predicate, scope, args);
    }

    /// <summary>
    ///     Exposes this filter as an asynchronous pull sink, reading candidate elements from
    ///     <paramref name="source" /> and exposing to the downstream only those fulfilling the predicate.
    /// </summary>
    /// <param name="source">The upstream asynchronous pull consumer to read candidate elements from.</param>
    /// <param name="target">The asynchronous pull consumer this component implements for the downstream.</param>
    /// <param name="context">The flow context providing cancellation for the pipeline.</param>
    public void Fuse(IAsyncConsumator<TSource> source, out IAsyncConsumator<TSource> target, FlowContext context)
    {
        target = new AsyncConsumator(source, predicate, scope, args);
    }

    /// <summary>
    ///     Exposes this filter as the upstream push input processor for an <see cref="IProducator{TSource}" /> source,
    ///     writing accepted elements into <paramref name="target" /> and proxying completion.
    /// </summary>
    /// <param name="source">The push side this component implements; written by the upstream source.</param>
    /// <param name="target">The downstream synchronous push producer to write the accepted elements into.</param>
    /// <param name="context">The flow context providing cancellation for the pipeline.</param>
    public void Fuse(out IProducator<TSource> source, IProducator<TSource> target, FlowContext context)
    {
        source = new Producator(target, predicate, scope, args);
    }

    /// <summary>
    ///     Exposes this filter as a synchronous pull sink, reading candidate elements from <paramref name="source" />
    ///     and exposing to the downstream only those fulfilling the predicate.
    /// </summary>
    /// <param name="source">The upstream synchronous pull consumer to read candidate elements from.</param>
    /// <param name="target">The synchronous pull consumer this component implements for the downstream.</param>
    /// <param name="context">The flow context providing cancellation for the pipeline.</param>
    public void Fuse(IConsumator<TSource> source, out IConsumator<TSource> target, FlowContext context)
    {
        target = new Consumator(source, predicate, scope, args);
    }

    private sealed class AsyncProducator(IAsyncProducator<TSource> writer, FilterPredicate<TSource, TScope, TArgs> predicate, TScope scope, TArgs args) : IAsyncProducator<TSource>
    {
        public bool TryWrite(TSource value)
        {
            if (writer.IsFull)
                return false;

            // An accepted value is forwarded; a rejected one is dropped but still acknowledged.
            return !predicate.Invoke(value, scope, args) || writer.TryWrite(value);
        }

        public bool TryComplete(Exception? ex = null) => writer.TryComplete(ex);

        public bool IsFull => writer.IsFull;

        public ValueTask<bool> WaitToWriteAsync() => writer.WaitToWriteAsync();
    }

    private sealed class Producator(IProducator<TSource> writer, FilterPredicate<TSource, TScope, TArgs> predicate, TScope scope, TArgs args) : IProducator<TSource>
    {
        public bool TryWrite(TSource value)
        {
            if (writer.IsFull)
                return false;

            return !predicate.Invoke(value, scope, args) || writer.TryWrite(value);
        }

        public bool TryComplete(Exception? ex = null) => writer.TryComplete(ex);

        public bool IsFull => writer.IsFull;
    }

    private sealed class AsyncConsumator(IAsyncConsumator<TSource> reader, FilterPredicate<TSource, TScope, TArgs> predicate, TScope scope, TArgs args) : IAsyncConsumator<TSource>
    {
        // Skip values that fail the predicate until one passes, a gap occurs or the upstream completes.
        public bool TryRead([MaybeNullWhen(false)] out TSource value)
        {
            while (reader.TryRead(out value))
            {
                if (predicate.Invoke(value, scope, args))
                    return true;
            }

            value = default;
            return false;
        }

        public bool IsCompleted => reader.IsCompleted;

        public ValueTask<bool> WaitToReadAsync() => reader.WaitToReadAsync();
    }

    private sealed class Consumator(IConsumator<TSource> reader, FilterPredicate<TSource, TScope, TArgs> predicate, TScope scope, TArgs args) : IConsumator<TSource>
    {
        public bool TryRead([MaybeNullWhen(false)] out TSource value)
        {
            while (reader.TryRead(out value))
            {
                if (predicate.Invoke(value, scope, args))
                    return true;
            }

            value = default;
            return false;
        }

        public bool IsCompleted => reader.IsCompleted;
    }
}
