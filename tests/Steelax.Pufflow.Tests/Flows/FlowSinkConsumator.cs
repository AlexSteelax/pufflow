using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Tests.Flows;

[Flow]
public partial class FlowSinkConsumator<T>
{
    /// <remarks>
    ///     Вытягивает данные из source
    /// </remarks>
    public void Execute(IConsumator<T> source, FlowContext context)
    {
        throw new NotImplementedException();
    }

    /// <remarks>
    ///     Вытягивает данные из source
    /// </remarks>
    public Task ExecuteAsync(IConsumator<T> source, FlowContext context)
    {
        throw new NotImplementedException();
    }
}