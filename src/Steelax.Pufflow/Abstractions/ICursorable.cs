using System.Diagnostics.CodeAnalysis;

namespace Steelax.Pufflow.Abstractions;

/// <summary>
///     Adds cursor semantics (peek without advance, explicit advance) to a pull consumer. A class may
///     additionally implement this interface to let a consumer inspect the current element, hold it until
///     it is fully handled, and only then advance past it.
/// </summary>
/// <typeparam name="T">The type of the consumed elements.</typeparam>
/// <remarks>
///     The cursor allows a consumer to "look at" the current element without committing to it, retrying as
///     long as needed (for example, until the output has capacity), and advancing only on success. The
///     cursor stays on the current element until <see cref="Advance" /> is called.
/// </remarks>
[PublicAPI]
public interface ICursorable<T>
    where T : allows ref struct
{
    /// <summary>
    ///     Peeks the current value without advancing; the cursor stays on this element until
    ///     <see cref="Advance" /> is called.
    /// </summary>
    /// <param name="value">The current value, if available.</param>
    /// <returns>
    ///     <see langword="true" /> when a value is available; otherwise <see langword="false" />.
    /// </returns>
    bool TryPeek([MaybeNullWhen(false)] out T value);

    /// <summary>
    ///     Advances the cursor to the next element after the current one was fully handled.
    /// </summary>
    void Advance();
    
    /// <summary>
    /// 
    /// </summary>
    bool IsCompleted { get; }
}