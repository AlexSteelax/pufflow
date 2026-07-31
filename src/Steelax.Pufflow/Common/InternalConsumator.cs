using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Common;

internal readonly struct InternalConsumator<T, TConsumator>(TConsumator consumator)
    where TConsumator : IConsumator<T>
{
    public TConsumator Handle() => consumator;
}
