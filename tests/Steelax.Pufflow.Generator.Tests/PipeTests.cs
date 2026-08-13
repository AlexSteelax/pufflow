using static Steelax.Pufflow.Generator.Tests.TestMarshal;

namespace Steelax.Pufflow.Generator.Tests;

public partial class PipeTests
{
    [Fact]
    public void PipeEnumerator_GeneratesFlowInterface()
    {
        var source = GetNoCompilationSource("MyTransform");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyTransform.g.cs"));
        Assert.NotNull(f);
        var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Pipe<System.Collections.Generic.IEnumerator<T1>, System.Collections.Generic.IEnumerator<T2>>>",
            c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void PipeAsyncEnumerator_GeneratesFlowInterface()
    {
        var source = GetNoCompilationSource("MyAsyncTransform");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyAsyncTransform.g.cs"));
        Assert.NotNull(f);
        var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Pipe<System.Collections.Generic.IAsyncEnumerator<T1>, System.Collections.Generic.IAsyncEnumerator<T2>>>",
            c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void PipeConsumator_GeneratesPipe()
    {
        var source = GetNoCompilationSource("MyPipeConsumator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyPipeConsumator.g.cs"));
        Assert.NotNull(f);
        var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Pipe<System.Collections.Generic.IEnumerator<T1>, Steelax.Pufflow.Abstractions.IConsumator<T2>>>",
            c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void Composite_GeneratesMultipleInterfaces()
    {
        var source = GetNoCompilationSource("MyComposite");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyComposite.g.cs"));
        Assert.NotNull(f);
        var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.IAsyncProducator<T1>, Steelax.Pufflow.Abstractions.IAsyncConsumator<T2>>>",
            c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void CompositeSync_GeneratesMultipleInterfaces()
    {
        // Two out parameters — the left producer (written by the upstream) and the right consumator
        // (read by the downstream): Fuse(out IProducator, out IConsumator, ctx).
        var source = GetNoCompilationSource("MyCompositeSync");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyCompositeSync.g.cs"));
        Assert.NotNull(f);
        var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.IProducator<T1>, Steelax.Pufflow.Abstractions.IConsumator<T2>>>",
            c);
        Assert.Contains("FlowProdToCons", c);
        Assert.DoesNotContain("GetFlow", c);
    }
}
