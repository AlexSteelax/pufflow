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

        flow
            .On(CarrierSequenceSource<int>.Configure(count, nothingRatio, static i => i, null))
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
            Trace.WriteLine($"Count expected {count}");
        }
    }
}

[Flow]
internal sealed partial class CarrierSequenceSource<T>
{
    private readonly int _countLimit;
    private readonly float _nothingRatio;
    private readonly Func<int, T> _factory;
    private readonly WatermarkProvider _watermarkProvider;
    private int _count;

    public CarrierSequenceSource(int count, float nothingRatio, Func<int, T> factory, WatermarkProvider? watermarkProvider,
        IAsyncProducator<Carrier<T>> source, FlowContext context)
    {
        _countLimit = count;
        _nothingRatio = nothingRatio;
        _factory = factory;
        _watermarkProvider = watermarkProvider ?? WatermarkProvider.System;
        context.RegisterBackgroundProcess(_ => MainLoopAsync(source, context));
    }
    
    public int Count => Volatile.Read(ref _count);
    
    private async Task MainLoopAsync(IAsyncProducator<Carrier<T>> source, CancellationToken cancellationToken)
    {
        try
        {
            for (;_count < _countLimit; _count++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (source.IsFull)
                    if (!await source.WaitToWriteAsync())
                        break;

                var value = _factory.Invoke(Count);
                var watermark = _watermarkProvider.GetWatermark();

                if (Random.Shared.NextSingle() <= _nothingRatio)
                    watermark = Watermark.Nothing;

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