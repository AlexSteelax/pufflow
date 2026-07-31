using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Tests.Flows;

[Flow]
public partial class FlowSourceProducator<T>
{
    /// <remarks>
    /// Толкает данные в target
    /// </remarks>
    public void Execute(IProducator<T> target, FlowContext context)
    {
        throw new NotImplementedException();
    }
    
    /// <remarks>
    /// Толкает данные в target
    /// </remarks>
    public void Execute(IAsyncProducator<T> target, FlowContext context)
    {
        throw new NotImplementedException();
    }
}