using static Steelax.Pufflow.Generator.Tests.TestMarshal;

namespace Steelax.Pufflow.Generator.Tests;

public partial class PipeTests
{
    [Fact]
    public void PipeConsumatorToEnumerator_GeneratesPipe()
    {
        var source = GetNoCompilationSource("MyPipeConsumatorToEnumerator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyPipeConsumatorToEnumerator.g.cs"));
        Assert.NotNull(f);
        var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.IConsumator<T1>, System.Collections.Generic.IEnumerator<T2>>>",
            c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void PipeConsumatorToAsyncEnumerator_GeneratesPipe()
    {
        var source = GetNoCompilationSource("MyPipeConsumatorToAsyncEnumerator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t =>
            t.FilePath.EndsWith("MyPipeConsumatorToAsyncEnumerator.g.cs"));
        Assert.NotNull(f);
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.IConsumator<T1>, System.Collections.Generic.IAsyncEnumerator<T2>>>",
            f.GetText(TestContext.Current.CancellationToken).ToString());
    }

    [Fact]
    public void PipeConsumatorToConsumator_GeneratesPipe()
    {
        var source = GetNoCompilationSource("MyPipeConsumatorToConsumator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyPipeConsumatorToConsumator.g.cs"));
        Assert.NotNull(f);
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.IConsumator<T1>, Steelax.Pufflow.Abstractions.IConsumator<T2>>>",
            f.GetText(TestContext.Current.CancellationToken).ToString());
    }

    [Fact]
    public void PipeConsumatorToAsyncConsumator_GeneratesPipe()
    {
        var source = GetNoCompilationSource("MyPipeConsumatorToAsyncConsumator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t =>
            t.FilePath.EndsWith("MyPipeConsumatorToAsyncConsumator.g.cs"));
        Assert.NotNull(f);
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.IConsumator<T1>, Steelax.Pufflow.Abstractions.IAsyncConsumator<T2>>>",
            f.GetText(TestContext.Current.CancellationToken).ToString());
    }

    [Fact]
    public void PipeAsyncConsumatorToEnumerator_GeneratesPipe()
    {
        var source = GetNoCompilationSource("MyPipeAsyncConsumatorToEnumerator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t =>
            t.FilePath.EndsWith("MyPipeAsyncConsumatorToEnumerator.g.cs"));
        Assert.NotNull(f);
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.IAsyncConsumator<T1>, System.Collections.Generic.IEnumerator<T2>>>",
            f.GetText(TestContext.Current.CancellationToken).ToString());
    }

    [Fact]
    public void PipeAsyncConsumatorToConsumator_GeneratesPipe()
    {
        var source = GetNoCompilationSource("MyPipeAsyncConsumatorToConsumator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t =>
            t.FilePath.EndsWith("MyPipeAsyncConsumatorToConsumator.g.cs"));
        Assert.NotNull(f);
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.IAsyncConsumator<T1>, Steelax.Pufflow.Abstractions.IConsumator<T2>>>",
            f.GetText(TestContext.Current.CancellationToken).ToString());
    }

    [Fact]
    public void PipeAsyncConsumatorToAsyncConsumator_GeneratesPipe()
    {
        var source = GetNoCompilationSource("MyPipeAsyncConsumatorToAsyncConsumator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t =>
            t.FilePath.EndsWith("MyPipeAsyncConsumatorToAsyncConsumator.g.cs"));
        Assert.NotNull(f);
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.IAsyncConsumator<T1>, Steelax.Pufflow.Abstractions.IAsyncConsumator<T2>>>",
            f.GetText(TestContext.Current.CancellationToken).ToString());
    }

    [Fact]
    public void PipeAsyncConsumatorToAsyncEnumerator_GeneratesPipe()
    {
        var source = GetNoCompilationSource("MyPipeAsyncConsumatorToAsyncEnumerator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t =>
            t.FilePath.EndsWith("MyPipeAsyncConsumatorToAsyncEnumerator.g.cs"));
        Assert.NotNull(f);
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.IAsyncConsumator<T1>, System.Collections.Generic.IAsyncEnumerator<T2>>>",
            f.GetText(TestContext.Current.CancellationToken).ToString());
    }

    [Fact]
    public void PipeConsumatorToAsyncProducator_GeneratesPipe()
    {
        var source = GetNoCompilationSource("MyPipeConsumatorToAsyncProducator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t =>
            t.FilePath.EndsWith("MyPipeConsumatorToAsyncProducator.g.cs"));
        Assert.NotNull(f);
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.IConsumator<T1>, Steelax.Pufflow.Abstractions.IAsyncProducator<T2>>>",
            f.GetText(TestContext.Current.CancellationToken).ToString());
    }
}
