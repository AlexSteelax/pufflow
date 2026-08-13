using System.Runtime.CompilerServices;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Kafka;

/// <summary>
///     Provides monotonic watermark values for the messages emitted by the pipeline.
/// </summary>
/// <remarks>
///     Use <see cref="System" /> for the default provider based on monotonic time; derive from this
///     class to supply custom watermark sources (for example, deterministic ones in tests).
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
        private long _lastWatermark = Watermark.NothingValue;
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override Watermark GetWatermark()
        {
            var watermark = Environment.TickCount64;

            if (_lastWatermark == watermark)
                return Watermark.Nothing();
            
            _lastWatermark = watermark;
            return Watermark.From(watermark);
        }
    }
}