using System.Runtime.CompilerServices;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators;

/// <summary>
///     Provides monotonic watermark values for the messages emitted by the pipeline.
/// </summary>
/// <remarks>
///     Use <see cref="System" /> for the default provider based on monotonic time; derive from this
///     class to supply custom watermark sources (for example, deterministic ones in tests).
///     <para />
///     A watermark is a progress marker, not a per-message timestamp: several consecutive messages
///     may share the same watermark (the value repeats while the underlying clock is inside one tick).
///     The reader treats each received watermark as a progress threshold — reaching it confirms all
///     messages with an equal or lower watermark, so repeated values are expected and harmless.
/// </remarks>
[PublicAPI]
public abstract class WatermarkProvider
{
    /// <summary>The system watermark provider based on monotonic time (<see cref="Environment.TickCount64" />).</summary>
    public static WatermarkProvider System => new SystemWatermarkProvider();

    /// <summary>Returns a fresh watermark value.</summary>
    /// <returns>The watermark to attach to the next emitted record.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public abstract Watermark GetWatermark();

    private sealed class SystemWatermarkProvider : WatermarkProvider
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override Watermark GetWatermark() => Watermark.FromEnvironmentTicks();
    }
}