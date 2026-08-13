using System.Reflection;

namespace Steelax.Pufflow;

/// <summary>
///     Provides reflection-based utilities for invoking dataflow handler methods at runtime.
/// </summary>
/// <remarks>
///     Bridges the unified <c>Fuse(...)</c> component contract to runtime invocation. The method is
///     resolved by name and signature (flow parameters followed by a <see cref="FlowContext" />) and
///     invoked with the supplied flow objects. Legacy <c>Get*</c> handlers are handled by
///     <see cref="FlowMetaNode" /> during node resolution.
/// </remarks>
public static class FlowMarshal
{
    /// <summary>
    ///     Invokes the <c>Fuse(...)</c> method on the specified instance, passing the given flow objects
    ///     followed by the context.
    /// </summary>
    /// <param name="instance">The component instance.</param>
    /// <param name="context">The flow context for cancellation.</param>
    /// <param name="inputs">
    ///     The flow objects passed to <c>Fuse</c> (an <c>out</c> parameter is pre-created by the caller or
    ///     passed as a boxed value that the method replaces).
    /// </param>
    public static void InvokeFuse(object instance, FlowContext context, params object[] inputs)
    {
        var method = instance.GetType().GetMethod("Fuse", BindingFlags.Public | BindingFlags.Instance);
        if (method is null)
            throw new MissingMethodException(instance.GetType().FullName, "Fuse");

        var parameters = new object[inputs.Length + 1];
        Array.Copy(inputs, parameters, inputs.Length);
        parameters[^1] = context;

        method.Invoke(instance, parameters);
    }
}