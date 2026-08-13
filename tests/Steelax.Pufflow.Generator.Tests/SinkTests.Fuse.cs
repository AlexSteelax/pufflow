using static Steelax.Pufflow.Generator.Tests.TestMarshal;

namespace Steelax.Pufflow.Generator.Tests;

public partial class SinkTests
{
    [Fact]
    public void SinkPullFuse_GeneratesSink()
    {
        var source = GetNoCompilationSource("MySinkPull");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MySinkPull.g.cs"));
        Assert.NotNull(f);
        var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Sink<System.Collections.Generic.IEnumerator<T>>>",
            c);
        Assert.Contains("FlowEnum", c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void SinkAsyncPullFuse_GeneratesSink()
    {
        var source = GetNoCompilationSource("MySinkAsyncPull");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MySinkAsyncPull.g.cs"));
        Assert.NotNull(f);
        var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Sink<System.Collections.Generic.IAsyncEnumerator<T>>>",
            c);
        Assert.Contains("FlowAEnum", c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void SinkConsumatorFuse_GeneratesSink()
    {
        var source = GetNoCompilationSource("MySinkConsumator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MySinkConsumator.g.cs"));
        Assert.NotNull(f);
        var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Sink<Steelax.Pufflow.Abstractions.IConsumator<T>>>",
            c);
        Assert.Contains("FlowCons", c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void SinkAsyncConsumatorFuse_GeneratesSink()
    {
        var source = GetNoCompilationSource("MySinkAsyncConsumator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MySinkAsyncConsumator.g.cs"));
        Assert.NotNull(f);
        var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Sink<Steelax.Pufflow.Abstractions.IAsyncConsumator<T>>>",
            c);
        Assert.Contains("FlowACons", c);
    }
}
