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

    public class LinkedToken
    {
        [Fact]
        public void ExternalCancellation_ShouldCancelContext()
        {
            using var cts = new CancellationTokenSource();
            using var source = new FlowSource(cts.Token);
            var ctx = source.Context;

            Assert.False(ctx.Token.IsCancellationRequested);

            cts.Cancel();

            Assert.True(ctx.Token.IsCancellationRequested);
        }

        [Fact]
        public void Done_ShouldCancelContextWithLinkedToken()
        {
            using var cts = new CancellationTokenSource();
            using var source = new FlowSource(cts.Token);
            var ctx = source.Context;

            ctx.Cancel();

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