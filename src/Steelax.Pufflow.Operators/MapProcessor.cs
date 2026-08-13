using System.Diagnostics.CodeAnalysis;

namespace Steelax.Pufflow.Operators;

/// <summary>
///     A dataflow pipe that applies a synchronous transform to each element of an async pull stream.
/// </summary>
/// <typeparam name="T1">The input element type.</typeparam>
/// <typeparam name="T2">The output element type.</typeparam>
/// <param name="transform">The function applied to each input element to produce the output element.</param>
/// <remarks>
///     Consumes an <see cref="IAsyncConsumator{T}" /> and produces an <see cref="IAsyncConsumator{T}" />,
///     mapping each value as it is read. The stream's completion and errors are forwarded as-is.
/// </remarks>
[Flow]
public sealed partial class MapProcessor<T1, T2>(Func<T1, T2> transform)
{
    /// <summary>
    ///     Wraps the upstream source so that each read value is mapped through the configured transform.
    /// </summary>
    /// <param name="source">The upstream async consumator to read from.</param>
    /// <param name="context">The flow context (cancellation is delegated to the upstream source).</param>
    /// <returns>An <see cref="IAsyncConsumator{T2}" /> that yields the transformed values.</returns>
    [PublicAPI]
    public IAsyncConsumator<T2> GetAsyncConsumator(IAsyncConsumator<T1> source, FlowContext context)
    {
        return new Mapper(source, transform);
    }

    private sealed class Mapper(IAsyncConsumator<T1> source, Func<T1, T2> transform) : IAsyncConsumator<T2>
    {
        public bool TryRead([MaybeNullWhen(false)] out T2 value, out bool completed)
        {
            if (source.TryRead(out var original, out completed))
            {
                value = transform.Invoke(original);
                return true;
            }

            value = default!;
            return false;
        }

        public ValueTask WaitToReadAsync()
        {
            return source.WaitToReadAsync();
        }
    }
}