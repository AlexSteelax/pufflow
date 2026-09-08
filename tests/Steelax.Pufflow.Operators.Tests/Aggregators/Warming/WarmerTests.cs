using Steelax.Pufflow.Operators.Aggregators.Warming;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Warming;

/// <summary>
///     Unit tests for the <see cref="Warmer{TKey,TWarm}" /> class.
/// </summary>
public static partial class WarmerTests
{
    private static Warmer<int, string> Create(
        IJobFactory<int, string>? jobFactory = null,
        Action? onReady = null,
        int maxConcurrency = 2,
        int maxQueued = 4,
        int segmentCapacity = 2)
    {
        var warmer = new Warmer<int, string>(maxConcurrency, maxQueued, segmentCapacity, jobFactory ?? new WarmingHelper.SyncJobFactory());

        if (onReady is not null)
            warmer.OnReady += onReady;

        return warmer;
    }

    private static void AddKeys(Warmer<int, string> warmer, int[] keys, WatermarkProvider? watermarkProvider = null)
    {
        watermarkProvider ??= WatermarkProvider.System;
        
        foreach (var key in keys)
        {
            if (!warmer.CanAdd)
                throw new InvalidOperationException($"Cannot add key {key}");
            
            var watermark = watermarkProvider.GetWatermark();
            
            warmer.AddKey(key, watermark);
        }
    }
}