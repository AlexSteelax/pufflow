namespace Steelax.Pufflow.Operators.Tests;

public static partial class WarmProcessorTests
{
    public sealed class Cancellation
    {
        [Fact(Timeout = 10_000)]
        public async Task CancelWhileJobPending_CompletesBuffer()
        {
            // Warm-джоб не завершается: цикл WarmProcessor спит на fan-in, ожидая его.
            var job = new TcsJob();
            var policy = new TestPolicy(); // греет ключ 2

            await using var flow = new FlowSource();
            var context = flow.Context;
            var processor = CreateProcessor(CreateWarmer(new TcsJobFactory(job)), policy, new ListAccumulatorFactory());
            var output = processor.GetAsyncConsumator(
                new ListAsyncEnumerator<Watermarked<int>>([new Watermarked<int>(2, Watermark.From(20))]),
                context);

            // Дожидаемся запуска warm-джоба — к этому моменту цикл уснул в ожидании.
            await WaitUntilAsync(() => job.Started, context.Token);

            context.Cancel();

            // Буфер должен быть завершён (в finally) — ожидание завершается, TryRead даёт Completed.
            await output.WaitToReadAsync().AsTask()
                .WaitAsync(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
            _ = output.TryRead(out _, out var completed);
            Assert.True(completed);
        }
    }
}