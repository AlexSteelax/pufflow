using System.Runtime.InteropServices;

namespace Steelax.Pufflow.Operators.Common;

/// <summary>
///     A zero-sized marker type that represents "no data".
/// </summary>
/// <remarks>
///     Deliberately mirrors the functional <c>unit</c> type. It is used as an empty payload slot, for example a
///     <c>Unit</c> branch of a union (<c>Unio&lt;TValue, Unit&gt;</c>) representing a bare progress marker that flows
///     through a stream without carrying an actual value while still advancing the stream's progress/watermark.
/// </remarks>
[StructLayout(LayoutKind.Sequential, Size = 1)]
public struct Unit;
