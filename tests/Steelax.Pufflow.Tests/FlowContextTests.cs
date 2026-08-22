namespace Steelax.Pufflow.Tests;

public static class FlowContextTests
{
    public class DefaultContext
    {
        private readonly FlowContext _context = default;

        [Fact]
        public void Token_ShouldReturnNone()
        {
            Assert.Equal(CancellationToken.None, _context.Token);
        }

        [Fact]
        public void Done_ShouldNotThrow()
        {
            _context.Cancel();
        }

        [Fact]
        public void ImplicitConversion_ShouldReturnNone()
        {
            CancellationToken token = _context;
            Assert.Equal(CancellationToken.None, token);
        }
    }

    public class ActiveContext
    {
        [Fact]
        public void Token_ShouldReturnActiveToken()
        {
            using var source = new FlowSource();
            var ctx = source.Context;

            Assert.False(ctx.Token.IsCancellationRequested);
        }

        [Fact]
        public void Done_ShouldCancelToken()
        {
            using var source = new FlowSource();
            var ctx = source.Context;

            ctx.Cancel();

            Assert.True(ctx.Token.IsCancellationRequested);
        }

        [Fact]
        public void Done_MultipleCalls_ShouldNotThrow()
        {
            using var source = new FlowSource();
            var ctx = source.Context;

            ctx.Cancel();
            ctx.Cancel();
            ctx.Cancel();

            Assert.True(ctx.Token.IsCancellationRequested);
        }

        [Fact]
        public void ImplicitConversion_ShouldReturnActiveToken()
        {
            using var source = new FlowSource();
            var ctx = source.Context;

            CancellationToken token = ctx;
            Assert.False(token.IsCancellationRequested);

            ctx.Cancel();
            Assert.True(token.IsCancellationRequested);
        }
    }

    public class DisposedSource
    {
        [Fact]
        public void Token_ShouldReturnCanceledToken()
        {
            var source = new FlowSource();
            var ctx = source.Context;
            source.Dispose();

            Assert.True(ctx.Token.IsCancellationRequested);
        }

        [Fact]
        public void Done_ShouldNotThrow()
        {
            var source = new FlowSource();
            var ctx = source.Context;
            source.Dispose();

            ctx.Cancel();
        }

        [Fact]
        public void ImplicitConversion_ShouldReturnCanceledToken()
        {
            var source = new FlowSource();
            var ctx = source.Context;
            source.Dispose();

            CancellationToken token = ctx;
            Assert.True(token.IsCancellationRequested);
        }
    }

    public class ExecuteAsyncCancellation
    {
        [Fact(Timeout = 5_000)]
        public async Task ExternalCancellation_ShouldCancelContext()
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
            await using var source = new FlowSource();
            var ctx = source.Context;

            // A background task that waits for the context token, so ExecuteAsync does not complete on its own.
            _ = source.RegisterBackground(async () => await Task.Delay(Timeout.InfiniteTimeSpan, ctx.Token));

            var runTask = source.ExecuteAsync(cts.Token);

            Assert.False(ctx.Token.IsCancellationRequested);

            await cts.CancelAsync();

            // The external cancellation cascades into the flow token: the background task is canceled and
            // ExecuteAsync faults with the cancellation.
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => runTask);
        }

        [Fact(Timeout = 5_000)]
        public async Task ContextCancellation_ShouldStopPipeline()
        {
            await using var source = new FlowSource();
            var ctx = source.Context;

            _ = source.RegisterBackground(async () => await Task.Delay(Timeout.InfiniteTimeSpan, ctx.Token));

            var runTask = source.ExecuteAsync(TestContext.Current.CancellationToken);

            Assert.False(runTask.IsCompleted);

            ctx.Cancel();

            // Cancelling the context is a normal pipeline stop (like DisposeAsync): the background task
            // is canceled and ExecuteAsync completes successfully — internal cancellation is not surfaced
            // as an exception.
            await runTask;

            Assert.True(ctx.Token.IsCancellationRequested);
        }
    }

    public class AsyncDisposal
    {
        [Fact]
        public async Task DisposeAsync_ShouldCancelContext()
        {
            var source = new FlowSource();
            var ctx = source.Context;

            await source.DisposeAsync();
            // Source is disposed; ctx still holds reference to the CTS

            Assert.True(ctx.Token.IsCancellationRequested);
        }

        [Fact]
        public async Task Done_AfterDisposeAsync_ShouldNotThrow()
        {
            var source = new FlowSource();
            var ctx = source.Context;

            await source.DisposeAsync();
            // Source disposed, ctx should still be safe

            ctx.Cancel();
        }
    }

    public class ThreadSafety
    {
        [Fact]
        public void ConcurrentDone_ShouldNotThrow()
        {
            using var source = new FlowSource();
            var ctx = source.Context;

            var actions = Enumerable
                .Range(0, 10)
                .Select(_ => (Action)ctx.Cancel)
                .ToArray();

            Assert.Multiple(actions);
        }

        [Fact]
        public void ConcurrentTokenRead_ShouldNotThrow()
        {
            using var source = new FlowSource();
            var ctx = source.Context;

            static void ReadToken(FlowContext c)
            {
                _ = c.Token;
            }

            var actions = new Action[10];

            for (var i = 0; i < actions.Length; i++)
                actions[i] = () => ReadToken(ctx);

            Assert.Multiple(actions);
        }
    }
}