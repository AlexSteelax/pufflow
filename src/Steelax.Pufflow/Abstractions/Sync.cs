using System.Runtime.InteropServices;

namespace Steelax.Pufflow.Abstractions;

/// <summary>
///     A zero-size marker struct that represents the synchronous dataflow mode.
/// </summary>
/// <remarks>
///     Used as a generic type argument in <see cref="Source{TKind,T}" />,
///     <see cref="Sink{TKind,T}" />, and <see cref="Pipe{TKind,TLeft,TRight}" />
///     to disambiguate synchronous and asynchronous pipeline stages at compile time.
///     The <c>Size = 0</c> layout ensures no runtime overhead.
/// </remarks>
[StructLayout(LayoutKind.Sequential, Size = 0)]
public struct Sync;