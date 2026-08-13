using static Steelax.Pufflow.Generator.Tests.TestMarshal;

namespace Steelax.Pufflow.Generator.Tests;

public partial class PipeTests
{
    [Fact]
    public void PipeProducatorToProducator_GeneratesPipe()
    {
        var source = GetNoCompilationSource("MyPipeProducator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyPipeProducator.g.cs"));
        Assert.NotNull(f);
        var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.IProducator<T1>, Steelax.Pufflow.Abstractions.IProducator<T2>>>",
            c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void PipeEnumeratorToAsyncConsumator_GeneratesPipe()
    {
        var source = GetNoCompilationSource("MyPipeEnumeratorToAsyncConsumator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t =>
            t.FilePath.EndsWith("MyPipeEnumeratorToAsyncConsumator.g.cs"));
        Assert.NotNull(f);
        var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Pipe<System.Collections.Generic.IEnumerator<T1>, Steelax.Pufflow.Abstractions.IAsyncConsumator<T2>>>",
            c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void PipeEnumeratorToAsyncEnumerator_GeneratesPipe()
    {
        var source = GetNoCompilationSource("MyPipeEnumeratorToAsyncEnumerator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t =>
            t.FilePath.EndsWith("MyPipeEnumeratorToAsyncEnumerator.g.cs"));
        Assert.NotNull(f);
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Pipe<System.Collections.Generic.IEnumerator<T1>, System.Collections.Generic.IAsyncEnumerator<T2>>>",
            f.GetText(TestContext.Current.CancellationToken).ToString());
    }

    [Fact]
    public void PipeEnumToAsyncProducator_GeneratesPipe()
    {
        var source = GetNoCompilationSource("MyPipeEnumToAsyncProducator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyPipeEnumToAsyncProducator.g.cs"));
        Assert.NotNull(f);
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Pipe<System.Collections.Generic.IEnumerator<T1>, Steelax.Pufflow.Abstractions.IAsyncProducator<T2>>>",
            f.GetText(TestContext.Current.CancellationToken).ToString());
    }

    [Fact]
    public void PipeAsyncEnumToAsyncProducator_GeneratesPipe()
    {
        var source = GetNoCompilationSource("MyPipeAsyncEnumToAsyncProducator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t =>
            t.FilePath.EndsWith("MyPipeAsyncEnumToAsyncProducator.g.cs"));
        Assert.NotNull(f);
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Pipe<System.Collections.Generic.IAsyncEnumerator<T1>, Steelax.Pufflow.Abstractions.IAsyncProducator<T2>>>",
            f.GetText(TestContext.Current.CancellationToken).ToString());
    }

    [Fact]
    public void PipeAsyncEnumToProducator_GeneratesPipe()
    {
        var source = GetNoCompilationSource("MyPipeAsyncEnumToProducator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyPipeAsyncEnumToProducator.g.cs"));
        Assert.NotNull(f);
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Pipe<System.Collections.Generic.IAsyncEnumerator<T1>, Steelax.Pufflow.Abstractions.IProducator<T2>>>",
            f.GetText(TestContext.Current.CancellationToken).ToString());
    }

    [Fact]
    public void PipeAsyncConsumatorToProducator_GeneratesPipe()
    {
        var source = GetNoCompilationSource("MyPipeAsyncConsumatorToProducator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t =>
            t.FilePath.EndsWith("MyPipeAsyncConsumatorToProducator.g.cs"));
        Assert.NotNull(f);
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.IAsyncConsumator<T1>, Steelax.Pufflow.Abstractions.IProducator<T2>>>",
            f.GetText(TestContext.Current.CancellationToken).ToString());
    }

    [Fact]
    public void PipeAsyncConsumatorToAsyncProducator_GeneratesPipe()
    {
        var source = GetNoCompilationSource("MyPipeAsyncConsumatorToAsyncProducator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t =>
            t.FilePath.EndsWith("MyPipeAsyncConsumatorToAsyncProducator.g.cs"));
        Assert.NotNull(f);
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.IAsyncConsumator<T1>, Steelax.Pufflow.Abstractions.IAsyncProducator<T2>>>",
            f.GetText(TestContext.Current.CancellationToken).ToString());
    }
}
