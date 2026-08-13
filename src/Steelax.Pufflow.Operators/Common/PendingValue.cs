namespace Steelax.Pufflow.Operators.Common;

/// <summary>
/// 
/// </summary>
/// <typeparam name="T"></typeparam>
public readonly struct PendingValue<T>
{
    /// <summary>
    /// 
    /// </summary>
    public PendingValue()
    {
        Occupied = false;
        Value = default!;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="value"></param>
    public PendingValue(T value)
    {
        Occupied = true;
        Value = value;
    }
    
    /// <summary>
    /// 
    /// </summary>
    public readonly bool Occupied;

    /// <summary>
    /// 
    /// </summary>
    public readonly T Value;
}