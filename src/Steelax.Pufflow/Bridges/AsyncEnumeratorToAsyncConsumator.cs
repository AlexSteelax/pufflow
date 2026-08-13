using System.Diagnostics.CodeAnalysis;
using Steelax.Pufflow.Abstractions;
using Steelax.Toolkit.HighPerformance;
using Steelax.Toolkit.HighPerformance.Concurrency.Primitives;

namespace Steelax.Pufflow.Bridges;

/// <summary>
///     Adapts an <see cref="IAsyncEnumerator{T}" /> to the pull-based <see cref="IAsyncConsumator{T}" /> interface.
/// </summary>
/// <typeparam name="T">The type of the enumerated elements.</typeparam>
/// <typeparam name="TEnumerator">The concrete enumerator type (avoids interface dispatch at the call site).</typeparam>
/// <remarks>
///     <para>
///         The bridge drives the enumerator through a <see cref="EventEnumerator{T}" /> and signals source
///         readiness via a <see cref="FanInSlim" /> slot (see <see cref="EnumeratorReadyIndex" />). The current
///         element is inspected via <see cref="TryPeek" /> and the enumerator is advanced explicitly via
///         <see cref="Advance" />; <see cref="TryRead" /> combines both. <see cref="WaitToReadAsync" /> just waits
///         for a state change.
///     </para>
///     <para>
///         Three fan-in ownership modes are supported:
///         <list type="bullet">
///             <item>
///                 <description><b>Default</b> — owns a private fan-in; the bridge is fully self-contained.</description>
///             </item>
///             <item>
///                 <description>
///                     <b>Manual</b> — no fan-in and no signal; <see cref="TryPeek" /> and <see cref="Advance" />
///                     are polled by the owner.
///                 </description>
///             </item>
///             <item>
///                 <description>
///                     <b>SharedAwaiter</b> — signals a caller-supplied fan-in and can wait on it via
///                     <see cref="WaitToReadAsync" />.
///                 </description>
///             </item>
///         </list>
///     </para>
/// </remarks>
[PublicAPI]
public struct AsyncEnumeratorToAsyncConsumator<T, TEnumerator> : IAsyncConsumator<T>, ICursorable<T>
    where TEnumerator : IAsyncEnumerator<T>
{
    /// <summary>The fan-in slot used to signal source readiness (a high bit, kept away from processor slots).</summary>
    public const int EnumeratorReadyIndex = 31;

    /// <summary>Fan-in ownership modes.</summary>
    private enum Mode : byte
    {
        /// <summary>Owns a private fan-in; self-contained.</summary>
        Default,

        /// <summary>No fan-in and no signal; <see cref="TryPeek" /> and <see cref="Advance" /> are polled by the owner.</summary>
        Manual,

        /// <summary>Signals a caller-supplied fan-in and can wait on it via <see cref="WaitToReadAsync" />.</summary>
        SharedAwaiter
    }

    private readonly EventEnumerator<T> _enumerator;
    private readonly FanInSlim? _fanIn;
    private readonly Mode _mode;

    /// <summary>
    ///     Creates a bridge that signals the caller-supplied <paramref name="fanIn" /> on source readiness.
    /// </summary>
    /// <param name="enumerator">The upstream enumerator to adapt.</param>
    /// <param name="fanIn">The shared fan-in whose <see cref="EnumeratorReadyIndex" /> slot is signaled.</param>
    public AsyncEnumeratorToAsyncConsumator(TEnumerator enumerator, FanInSlim fanIn)
    {
        _fanIn = fanIn;
        _enumerator = enumerator.AsNonBlocking();
        _enumerator.OnReady += _fanIn.GetSignalCallback(EnumeratorReadyIndex).Handler;
        _mode = Mode.SharedAwaiter;
    }

    /// <summary>
    ///     Creates a bridge that either owns a private fan-in or signals nothing at all.
    /// </summary>
    /// <param name="enumerator">The upstream enumerator to adapt.</param>
    /// <param name="manual">
    ///     When <see langword="true" />, no fan-in is created and no signal is raised; <see cref="TryPeek" />
    ///     and <see cref="Advance" /> must be polled by the owner. When <see langword="false" />, a private
    ///     fan-in is created and the bridge is self-contained.
    /// </param>
    public AsyncEnumeratorToAsyncConsumator(TEnumerator enumerator, bool manual)
    {
        if (manual)
        {
            _fanIn = null;
            _enumerator = enumerator.AsNonBlocking();
            _mode = Mode.Manual;
        }
        else
        {
            _fanIn = new FanInSlim();
            _enumerator = enumerator.AsNonBlocking();
            _enumerator.OnReady += _fanIn.GetSignalCallback(EnumeratorReadyIndex).Handler;
            _mode = Mode.Default;
        }
    }

    /// <summary>
    ///     Peeks the current value without advancing; the enumerator stays on this element until
    ///     <see cref="Advance" /> is called.
    /// </summary>
    /// <param name="value">The current value, if available.</param>
    /// <param name="completed">
    ///     Set to <see langword="true" /> when the stream has ended; otherwise <see langword="false" />.
    ///     When the return value is <see langword="false" /> and <paramref name="completed" /> is
    ///     <see langword="false" />, the current iteration is still pending.
    /// </param>
    /// <returns>
    ///     <see langword="true" /> when a value is available; otherwise <see langword="false" />.
    /// </returns>
    /// <remarks>
    ///     The first call primes the underlying enumerator (the initial <c>MoveNext</c> is deferred until
    ///     the first peek). A faulted or canceled iteration is rethrown here (via the underlying result),
    ///     mirroring <see cref="TryRead" />.
    /// </remarks>
    public bool TryPeek([MaybeNullWhen(false)] out T value, out bool completed)
    {
        var state = _enumerator.GetState();

        if (state == default)
        {
            // Prime the first iteration.
            _enumerator.MoveNext();
            state = _enumerator.GetState();
        }

        if (state.IsCompletedSuccessfully || state.IsCanceled || state.IsFaulted)
        {
            // Result throws for canceled/faulted iterations, propagating the outcome to the consumer.
            value = _enumerator.GetResult();
            completed = false;
            return true;
        }

        if (state.IsPending)
        {
            value = default!;
            completed = false;
            return false;
        }

        if (state.IsEndOfStream)
        {
            value = default!;
            completed = true;
            return false;
        }

        // This code path must be unreachable.
        throw new InvalidOperationException("The enumerator returned an invalid state.");
    }

    /// <summary>
    ///     Advances to the next iteration after the current element was fully handled.
    /// </summary>
    /// <remarks>
    ///     Starts the next <c>MoveNext</c>; its completion signals the fan-in. The source-ready bit of the
    ///     consumed element is cleared here — redundant when an external owner consumed it via <c>Take</c>.
    /// </remarks>
    public void Advance()
    {
        _fanIn?.TryReset(EnumeratorReadyIndex);
        _enumerator.MoveNext();
    }

    /// <inheritdoc />
    public bool TryRead([MaybeNullWhen(false)] out T value, out bool completed)
    {
        if (!TryPeek(out value, out completed))
            return false;

        Advance();
        return true;
    }

    /// <summary>Waits for the source to become ready.</summary>
    /// <returns>A task that completes when the source may have a value ready.</returns>
    /// <remarks>
    ///     In <see cref="Mode.Manual" /> mode this never waits — the owner polls <see cref="TryPeek" /> and
    ///     <see cref="Advance" />. In the other modes it waits on the fan-in while the current iteration is
    ///     pending; a readiness bit consumed by an external owner (via <c>Take</c>) is handled by the owner
    ///     itself.
    /// </remarks>
    public ValueTask WaitToReadAsync()
    {
        if (_enumerator.GetState().IsPending)
            switch (_mode)
            {
                case Mode.SharedAwaiter:
                case Mode.Default:
                    return _fanIn!.WaitAsync();
                case Mode.Manual:
                default:
                    break;
            }

        return ValueTask.CompletedTask;
    }
}