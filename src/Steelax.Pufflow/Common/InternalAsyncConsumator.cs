using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Common;

internal readonly struct InternalAsyncConsumator<T, TAsyncConsumator>(TAsyncConsumator consumator)
    where TAsyncConsumator : IAsyncConsumator<T>
{
    public TAsyncConsumator GetAsyncConsumator(CancellationToken cancellationToken = default) => consumator;
}
