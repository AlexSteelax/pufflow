using System.Reflection;

namespace Steelax.Pufflow;

public static class FlowMarshal
{
    public static object? GetAsyncEnumerator(object instance, FlowContext context, object? inputEnumerator = null)
    {
        var type = instance.GetType();

        if (type.ImplementsGenericInterface(typeof(IAsyncEnumerator<>)))
        {
            return instance;
        }

        var method = type.GetMethod("GetAsyncEnumerator", BindingFlags.Public | BindingFlags.Instance)!;
        var enumerator = inputEnumerator is null
            ? method.Invoke(instance, [context])!
            : method.Invoke(instance, [inputEnumerator, context])!;
            
        enumerator = Convert.ChangeType(enumerator, enumerator.GetType());
            
        return enumerator;
    }

    public static object? GetExecuteAsync(object instance, FlowContext context, object? inputEnumerator = null)
    {
        var type = instance.GetType();

        if (type == typeof(Func<object>))
        {
            return instance;
        }
        
        var method = type.GetMethod("ExecuteAsync", BindingFlags.Public | BindingFlags.Instance)!;

        return (Func<object>?)Handler;

        object Handler() =>
            inputEnumerator is null
                ? method.Invoke(instance, [context])!
                : method.Invoke(instance, [inputEnumerator, context])!;
    }
    
    private static bool ImplementsGenericInterface(this Type type, Type interfaceType)
    {
        if (!interfaceType.IsInterface || !interfaceType.IsGenericTypeDefinition)
            throw new ArgumentException("interfaceType должен быть открытым дженерик-интерфейсом (например, typeof(IRepository<>))");

        if (type.IsGenericType && type.GetGenericTypeDefinition() == interfaceType)
            return true;
        
        return type.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == interfaceType);
    }
}