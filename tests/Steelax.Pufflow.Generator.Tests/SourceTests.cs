using static Steelax.Pufflow.Generator.Tests.TestMarshal;

namespace Steelax.Pufflow.Generator.Tests;

public partial class SourceTests
{
    [Fact]
    public void SourceEnumerator_GeneratesFlowInterface()
    {
        var source = GetNoCompilationSource("MySource");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MySource.g.cs"));
        Assert.NotNull(f);
        var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains("IFlowable<Steelax.Pufflow.Source<System.Collections.Generic.IEnumerator<T>>>", c);
        Assert.Contains("FlowEnum", c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void SourceAsyncEnumerator_GeneratesFlowInterface()
    {
        var source = GetNoCompilationSource("MyAsyncSource");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyAsyncSource.g.cs"));
        Assert.NotNull(f);
        var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains("IFlowable<Steelax.Pufflow.Source<System.Collections.Generic.IAsyncEnumerator<T>>>", c);
        Assert.Contains("FlowAEnum", c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void Consumator_GeneratesFlowInterface()
    {
        var source = GetNoCompilationSource("MyConsumator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyConsumator.g.cs"));
        Assert.NotNull(f);
        var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains("IFlowable<Steelax.Pufflow.Source<Steelax.Pufflow.Abstractions.IConsumator<T>>>", c);
        Assert.Contains("FlowCons", c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void SourceAsyncConsumator_GeneratesFlowInterface()
    {
        var source = GetNoCompilationSource("MySourceAsyncConsumator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MySourceAsyncConsumator.g.cs"));
        Assert.NotNull(f);
        var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains("IFlowable<Steelax.Pufflow.Source<Steelax.Pufflow.Abstractions.IAsyncConsumator<T>>>", c);
        Assert.Contains("FlowACons", c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void FuseSourceAsyncEnumerator_GeneratesSource()
    {
        var source = GetNoCompilationSource("MyFuseSource");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyFuseSource.g.cs"));
        Assert.NotNull(f);
        var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains("IFlowable<Steelax.Pufflow.Source<System.Collections.Generic.IAsyncEnumerator<T>>>", c);
        Assert.Contains("FlowAEnum", c);
        Assert.DoesNotContain("GetFlow", c);
    }
}
