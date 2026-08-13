using System.Runtime.InteropServices;

namespace Steelax.Pufflow.Operators;

/// <summary>
///     A 1-byte marker struct that signals a timeout-based await behavior.
/// </summary>
/// <remarks>
///     Used as a marker by operators that need to await an asynchronous operation
///     with a configurable timeout (for example, <c>Timeout</c>). The <c>Size = 1</c>
///     layout guarantees the marker carries no runtime overhead.
/// </remarks>
[StructLayout(LayoutKind.Sequential, Size = 1)]
public readonly struct AwaitTimeout;