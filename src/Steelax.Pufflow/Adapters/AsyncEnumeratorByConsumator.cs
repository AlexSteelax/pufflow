using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Adapters;

/// <summary>
///     Adapts an <see cref="IAsyncConsumator{T}" /> into a standard .NET
///     <see cref="IAsyncEnumerator{T}" />. Values are pulled on demand; faults from the consumator are
///     propagated through <see cref="MoveNextAsync" />, and completion is surfaced as end-of-stream.
/// </summary>
/// <typeparam name="T">The element type flowing through the stream.</typeparam>
public sealed class AsyncEnumeratorByConsumator<T> : IAsyncEnumerator<T>
{
    private readonly IAsyncConsumator<T> _consumator;
    private T _current;

    /// <summary>Wraps the consumator, taking ownership of its consumption.</summary>
    /// <param name="consumator">The pull stream adapted into an enumerator.</param>
    public AsyncEnumeratorByConsumator(IAsyncConsumator<T> consumator)
    {
        _consumator = consumator;
        _current = default!;
    }

    /// <inheritdoc/>
    public ValueTask<bool> MoveNextAsync()
    {
        if (_consumator.TryRead(out var item))
        {
            _current = item!;
            return ValueTask.FromResult(true);
        }

        return _consumator.IsCompleted
            ? ValueTask.FromResult(false)
            : _consumator.WaitToReadAsync();
    }

    /// <inheritdoc/>
    public T Current => _current;

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}