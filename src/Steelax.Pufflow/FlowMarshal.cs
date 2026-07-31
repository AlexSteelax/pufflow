using System.Reflection;

namespace Steelax.Pufflow;

/// <summary>
/// Provides reflection-based utilities for invoking dataflow handler methods at runtime.
/// </summary>
/// <remarks>
/// This is a temporary bridge used by the <c>FlowExt.Next</c> extension methods
/// to dynamically call <c>GetAsyncEnumerator</c> and <c>ExecuteAsync</c> on user components
/// until proper compile-time code generation is implemented for all pipeline combinations.
/// </remarks>
public static class FlowMarshal
{
    /// <summary>
    /// Invokes the <c>GetAsyncEnumerator</c> method on the specified instance.
    /// </summary>
    /// <param name="instance">The component instance.</param>
    /// <param name="context">The flow context for cancellation.</param>
    /// <param name="inputEnumerator">
    /// An optional input enumerator to pass to the method.
    /// When null, the method is called with only the context parameter.
    /// </param>
    /// <returns>
    /// The result of the <c>GetAsyncEnumerator</c> invocation,
    /// or the instance itself if it already implements <see cref="IAsyncEnumerator{T}"/>.
    /// </returns>
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

    /// <summary>
    /// Wraps the <c>ExecuteAsync</c> method invocation into a <see cref="Func{TResult}"/> delegate.
    /// </summary>
    /// <param name="instance">The component instance.</param>
    /// <param name="context">The flow context for cancellation.</param>
    /// <param name="inputEnumerator">
    /// An optional input enumerator to pass to the method.
    /// When null, the method is called with only the context parameter.
    /// </param>
    /// <returns>
    /// A <see cref="Func{TResult}"/> that, when invoked, calls the <c>ExecuteAsync</c> method
    /// and returns the result as <see cref="object"/>.
    /// Returns the instance itself if it is already a <see cref="Func{TResult}"/>.
    /// </returns>
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

    /// <summary>
    /// Checks whether a type implements a specific generic interface definition.
    /// </summary>
    /// <param name="type">The type to inspect.</param>
    /// <param name="interfaceType">An open generic interface type definition (e.g., typeof(IAsyncEnumerator)).</param>
    /// <returns><see langword="true"/> if the type implements the interface; otherwise, <see langword="false"/>.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="interfaceType"/> is not an interface or not a generic type definition.</exception>
    private static bool ImplementsGenericInterface(this Type type, Type interfaceType)
    {
        if (!interfaceType.IsInterface || !interfaceType.IsGenericTypeDefinition)
            throw new ArgumentException("interfaceType must be an open generic interface (e.g., typeof(IAsyncEnumerable<>))", nameof(interfaceType));

        if (type.IsGenericType && type.GetGenericTypeDefinition() == interfaceType)
            return true;

        return type.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == interfaceType);
    }
}
