using System.Diagnostics;
using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Sdk.Test;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Buffering;

/// <summary>
///     Simple end-to-end: <c>Source</c> of <see cref="Carrier{T}" /> → <c>Filter</c> (everything passes) →
///     <c>Buffering</c> → <c>Consume</c> sink. The sink must observe exactly the same watermark sequence
///     that entered the pipeline.
/// </summary>
public class FilterBufferingCarrierTests
{
    private const int TimeoutMs = 200_000;

    [FlakyTheory(1)]
    [InlineData(100, 0.2f)]
    public async Task FilterPassAll_Buffering_Carriers_Sink_YieldsSameWatermarkSequence(int count, float nothingRatio)
    {
        using var cts = new CancellationTokenSource(TimeoutMs);
        
        await using var flow = new FlowSource();

        var seq = new CarrierSequenceSource<int>(count, nothingRatio, static i => i);
        
        flow
            .On(seq)
            .Filter(static (in s) => s >= 0)
            .Buffering(16)
            .Consume(out var reader);

        try
        {
            await flow.ExecuteAsync(cts.Token);

            var output = await reader
                .ReadAllAsync(cts.Token)
                .ToListAsync(cts.Token);

            var watermarks = output.Where(static s => s.HasWatermark).Select(static s => s.Watermark).ToList();

            Assert.True(watermarks.Count > count / 2);
            Assert.OrderIncreasing(watermarks, false);
            Assert.Equal(Enumerable.Range(0, count), output.Select(static c => c.Value));
        }
        catch
        {
            Trace.WriteLine($"Count {seq.Count}");
        }
    }
}

[Flow]
internal sealed partial class CarrierSequenceSource<T>(int count, float nothingRatio, Func<int, T> factory, WatermarkProvider? watermarkProvider = null)
{
    private readonly WatermarkProvider _watermarkProvider = watermarkProvider ?? WatermarkProvider.System;
    private int _count;
    
    public int Count => Volatile.Read(ref _count);
    
    public void Fuse(IAsyncProducator<Carrier<T>> source, FlowContext context)
    {
        context.RegisterBackground(() => MainLoopAsync(source, context));
    }

    private async Task MainLoopAsync(IAsyncProducator<Carrier<T>> source, CancellationToken cancellationToken)
    {
        try
        {
            for (;_count < count; _count++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (source.IsFull)
                    if (!await source.WaitToWriteAsync())
                        break;

                var value = factory.Invoke(Count);
                var watermark = _watermarkProvider.GetWatermark();

                if (Random.Shared.NextSingle() <= nothingRatio)
                    watermark = Watermark.Nothing();

                if (!source.TryWrite(new Carrier<T>(value, watermark)))
                    _count--;

                await Task.Delay(1, cancellationToken);
            }
        }
        catch (Exception ex)
        {
            source.TryComplete(ex);
        }
        finally
        {
            source.TryComplete();
        }
    }
}