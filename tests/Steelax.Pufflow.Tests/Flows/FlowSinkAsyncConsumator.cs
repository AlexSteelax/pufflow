using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Tests.Flows;

[Flow]
public partial class FlowSinkAsyncConsumator<T>
{
    /// <remarks>
    /// Вытягивает данные из source
    /// </remarks>
    public Task ExecuteAsync(IAsyncConsumator<T> source, FlowContext context)
    {
        throw new NotImplementedException();
    }
}