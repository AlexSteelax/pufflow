using System.Runtime.InteropServices;

namespace Steelax.Pufflow.Abstractions;

/// <summary>
/// Represents the result of a non-blocking read operation.
/// A 1-byte discriminated union: <see cref="Ready"/>, <see cref="Nothing"/>, <see cref="Completed"/>.
/// Implicitly converts to <see langword="bool"/> for use with <c>[MaybeNullWhen(false)]</c>.
/// </summary>
[PublicAPI]
[StructLayout(LayoutKind.Explicit, Size = 1)]
public readonly struct ReadResult
{
    [FieldOffset(0)]
    private readonly byte _kind;

    private ReadResult(byte kind) => _kind = kind;

    /// <summary>A value was successfully read.</summary>
    [PublicAPI] public static ReadResult Ready() => new(0);

    /// <summary>No value is currently available; the stream is still active.</summary>
    [PublicAPI] public static ReadResult Nothing() => new(1);

    /// <summary>The stream has ended; no more values will be available.</summary>
    [PublicAPI] public static ReadResult Completed() => new(2);

    /// <summary>Indicates whether a value was successfully read.</summary>
    [PublicAPI] public bool IsReady => _kind == 0;

    /// <summary>Indicates no value is available yet; retry later.</summary>
    [PublicAPI] public bool IsNothing => _kind == 1;

    /// <summary>Indicates the stream has ended.</summary>
    [PublicAPI] public bool IsCompleted => _kind == 2;

    /// <summary><c>true</c> for <see cref="Ready"/>; otherwise <c>false</c>.</summary>
    [PublicAPI]
    public static implicit operator bool(ReadResult result) => result.IsReady;
}
