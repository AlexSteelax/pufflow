using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Tests.Flows;

[Flow]
public partial class FlowPipeProducator<T1, T2>
{
    /// <remarks>
    ///     Тянет данные из source и толкает данные в target
    /// </remarks>
    public void Fuse(IEnumerator<T1> source, IProducator<T2> target, FlowContext context)
    {
        throw new NotImplementedException();
    }

    /// <remarks>
    ///     Тянет данные из source и толкает данные в target
    /// </remarks>
    public void Fuse(IEnumerator<T1> source, IAsyncProducator<T2> target, FlowContext context)
    {
        throw new NotImplementedException();
    }
    
    /// <remarks>
    ///     Тянет данные из source и толкает данные в target
    /// </remarks>
    public void Fuse(IConsumator<T1> source, IProducator<T2> target, FlowContext context)
    {
        throw new NotImplementedException();
    }

    /// <remarks>
    ///     Тянет данные из source и толкает данные в target
    /// </remarks>
    public void Fuse(IConsumator<T1> source, IAsyncProducator<T2> target, FlowContext context)
    {
        throw new NotImplementedException();
    }
    
    /// <remarks>
    ///     Тянет данные из source и толкает данные в target
    /// </remarks>
    public void Fuse(IAsyncEnumerator<T1> source, IProducator<T2> target, FlowContext context)
    {
        throw new NotImplementedException();
    }
    
    /// <remarks>
    ///     Тянет данные из source и толкает данные в target
    /// </remarks>
    public void Fuse(IAsyncEnumerator<T1> source, IAsyncProducator<T2> target, FlowContext context)
    {
        throw new NotImplementedException();
    }
    
    /// <remarks>
    ///     Тянет данные из source и толкает данные в target
    /// </remarks>
    public void Fuse(IAsyncConsumator<T1> source, IProducator<T2> target, FlowContext context)
    {
        throw new NotImplementedException();
    }
    
    /// <remarks>
    ///     Тянет данные из source и толкает данные в target
    /// </remarks>
    public void Fuse(IAsyncConsumator<T1> source, IAsyncProducator<T2> target, FlowContext context)
    {
        throw new NotImplementedException();
    }
}