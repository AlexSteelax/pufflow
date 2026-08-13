using Steelax.Toolkit.HighPerformance.Concurrency.Primitives;

namespace Steelax.Pufflow.Operators;

/// <summary>
///     A pipe component that decouples the upstream source from the downstream consumer by pumping
///     elements through a bounded in-memory buffer in a background task.
/// </summary>
/// <typeparam name="T">The type of elements flowing through the buffer.</typeparam>
/// <remarks>
///     <para>
///         The enumerator starts a background worker that reads the source and writes elements into the
///         buffer, honoring backpressure through the buffer's write side. The enumerator itself reads
///         elements from the buffer and yields them downstream, so a slow consumer does not stall the source
///         and a fast source does not overwhelm the consumer beyond the buffer's capacity.
///     </para>
///     <para>
///         When the source is exhausted, the worker completes the buffer, ending the consumer. If the source
///         faults, the fault is propagated through the buffer and rethrown downstream. Canceling the flow
///         context (or disposing the enumerator early) completes the buffer so both the worker and the
///         consumer stop.
///     </para>
/// </remarks>
[Flow]
public sealed partial class BufferProcessor<T>
{
    private readonly InternalEventQueue<T> _buffer;
    private readonly FanInSlim _fan;

    /// <summary>
    ///     Initializes a new instance of the <see cref="BufferProcessor{T}" /> class.
    /// </summary>
    /// <param name="capacity">The maximum number of elements buffered before backpressure is applied.</param>
    /// <exception cref="ArgumentOutOfRangeException">
    ///     Thrown when <paramref name="capacity" /> is not positive.
    /// </exception>
    public BufferProcessor(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);

        _buffer = new InternalEventQueue<T>(capacity);
        _fan = new FanInSlim();

        _buffer.OnWriteReady += _fan.GetSignalCallback(0).Handler;
    }

    /// <summary>
    ///     Returns an async enumerator that reads <paramref name="source" /> in the background and yields
    ///     buffered elements downstream.
    /// </summary>
    /// <param name="source">The upstream async enumerator to buffer.</param>
    /// <param name="context">The flow context providing cancellation for the pipeline.</param>
    /// <returns>An async enumerator yielding elements from the buffer.</returns>
    /// <exception cref="OperationCanceledException">
    ///     Thrown when the pipeline is canceled.
    /// </exception>
    /// <exception cref="Exception">
    ///     Rethrown when the source enumerator faults.
    /// </exception>
    public async IAsyncEnumerator<T> GetAsyncEnumerator(IAsyncEnumerator<T> source, FlowContext context)
    {
        // Cancel the pipeline by completing the buffer, which unblocks both the worker and this reader.
        await using var registration = context.Token.Register(() => _buffer.Complete());

        var worker = BackgroundWorker(source, context);

        try
        {
            while (true)
                // Drain whatever is already buffered, then wait only if the buffer is empty.
                if (_buffer.TryRead(out var item, out var completed))
                {
                    yield return item;
                }
                else
                {
                    if (completed)
                        break; // the worker completed the buffer

                    await _buffer.WaitToReadAsync();
                }

            // The worker completed the buffer; propagate its outcome (a source fault, if any).
            // WaitAsync with the flow token turns a canceled pipeline into OperationCanceledException
            // instead of hanging on a worker blocked on the source.
            await worker.WaitAsync(context.Token);
        }
        finally
        {
            // The consumer stopped early (disposal or cancellation): complete the buffer to stop the
            // worker. Completion is idempotent, and the worker treats a completed buffer as the end.
            _buffer.Complete();
        }
    }

    /// <summary>
    ///     Pumps elements from <paramref name="source" /> into the internal buffer until the source
    ///     is exhausted or the buffer is completed, propagating faults through the buffer completion.
    /// </summary>
    private async Task BackgroundWorker(IAsyncEnumerator<T> source, FlowContext context)
    {
        try
        {
            while (!context.Token.IsCancellationRequested && await source.MoveNextAsync())
            {
                var item = source.Current;

                while (!_buffer.TryWrite(item))
                {
                    await _fan.WaitAsync();

                    _ = _fan.Take();
                }
            }

            _buffer.Complete();
        }
        catch (Exception ex)
        {
            _buffer.Complete(ex);
            throw;
        }
    }
}