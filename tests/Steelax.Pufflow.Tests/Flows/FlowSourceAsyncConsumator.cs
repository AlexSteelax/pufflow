using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Tests.Flows;

[Flow]
public partial class FlowSourceAsyncConsumator<T>
{
    /// <remarks>
    ///     Отдает объект для вытягивания данных
    /// </remarks>
    public IAsyncConsumator<T> GetAsyncConsumator(FlowContext context)
    {
        throw new NotImplementedException();
    }
}