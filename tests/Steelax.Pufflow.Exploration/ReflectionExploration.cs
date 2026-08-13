using Xunit;

namespace Steelax.Pufflow.Exploration;

public class ReflectionExploration
{
    [Fact]
    public void FindInOutArgs()
    {
        var instance = new Test<int>();

        var method = instance.GetType().GetMethod("Fuse");

        Assert.NotNull(method);
        Assert.Equal(typeof(void), method.ReturnType);
        
        var args = method
            .GetParameters()
            .Select(static p => new
            {
                Type = p.ParameterType,
                IsIn = p.IsIn,
                IsOut = p.IsOut
            })
            .ToList();
        
        Assert.NotEmpty(args);

        var hasContext = args.Any(static arg => arg.Type == typeof(Context));
        
        Assert.True(hasContext);
        
        Assert.True(args[0].IsIn); // входной аргумент не обязан имять in маркер
        Assert.True(args[1].IsOut); // выходной обязан иметь out маркер
        
        Assert.False(args[2].IsIn);
        Assert.False(args[2].IsOut);

        object?[] parameters = [new AsyncEnumerator<int>(1), null, new Context()];

        method.Invoke(instance, parameters);

        var outParameter = parameters[1] as AsyncEnumerator<int>;

        Assert.NotNull(outParameter);
        
        Assert.Equal(2, outParameter.Id);
    }

    private sealed class Test<T>
    {
        public void Fuse(in IAsyncEnumerator<T> input, out IAsyncEnumerator<T> output, Context _)
        {
            output = new AsyncEnumerator<T>(2);
        }
    }

    private sealed class AsyncEnumerator<T>(int id) : IAsyncEnumerator<T>
    {
        public int Id => id;
        
        public ValueTask DisposeAsync() => throw new NotImplementedException();

        public ValueTask<bool> MoveNextAsync() => throw new NotImplementedException();

        public T Current => default!;
    }

    private sealed class Context;
}