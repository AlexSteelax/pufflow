using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Tests.Flows;

[Flow]
public partial class FlowSinkEnumerator<T>
{
    /// <remarks>
    /// Вытягивает данные из source
    /// </remarks>
    public void Execute(IEnumerator<T> source, FlowContext context)
    {
        throw new NotImplementedException();
    }
    
    /// <remarks>
    /// Вытягивает данные из source
    /// </remarks>
    public Task ExecuteAsync(IEnumerator<T> source, FlowContext context)
    {
        throw new NotImplementedException();
    }
}