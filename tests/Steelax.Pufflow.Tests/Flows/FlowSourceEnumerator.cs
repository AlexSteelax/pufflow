using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Tests.Flows;

[Flow]
public partial class FlowSourceEnumerator<T>
{
    /// <remarks>
    ///     Отдает объект для вытягивания данных
    /// </remarks>
    public IEnumerator<T> GetEnumerator(FlowContext context)
    {
        throw new NotImplementedException();
    }
}