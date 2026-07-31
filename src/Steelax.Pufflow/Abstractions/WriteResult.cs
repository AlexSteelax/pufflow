using System.Runtime.InteropServices;

namespace Steelax.Pufflow.Abstractions;

/// <summary>
/// Represents the result of a non-blocking write operation.
/// A 1-byte discriminated union: <see cref="Success"/>, <see cref="Overflow"/>.
/// Implicitly converts to <see langword="bool"/>.
/// </summary>
[PublicAPI]
[StructLayout(LayoutKind.Explicit, Size = 1)]
public readonly struct WriteResult
{
    [FieldOffset(0)]
    private readonly byte _kind;

    private WriteResult(byte kind) => _kind = kind;

    /// <summary>The value was successfully written.</summary>
    [PublicAPI] public static WriteResult Success() => new(0);

    /// <summary>The buffer is full; the value was not written.</summary>
    [PublicAPI] public static WriteResult Overflow() => new(1);

    /// <summary>Indicates whether the value was successfully written.</summary>
    [PublicAPI] public bool IsSuccess => _kind == 0;

    /// <summary>Indicates the buffer is full; retry later.</summary>
    [PublicAPI] public bool IsOverflow => _kind == 1;

    /// <summary><c>true</c> for <see cref="Success"/>; otherwise <c>false</c>.</summary>
    [PublicAPI]
    public static implicit operator bool(WriteResult result) => result.IsSuccess;
}
