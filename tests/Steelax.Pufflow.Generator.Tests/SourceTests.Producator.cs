using static Steelax.Pufflow.Generator.Tests.TestMarshal;

namespace Steelax.Pufflow.Generator.Tests;

public partial class SourceTests
{
    [Fact]
    public void SourcePushFuse_GeneratesSource()
    {
        var source = GetNoCompilationSource("MySourcePush");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MySourcePush.g.cs"));
        Assert.NotNull(f);
        var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Source<Steelax.Pufflow.Abstractions.IProducator<T>>>",
            c);
        Assert.Contains("FlowProd", c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void SourcePushAsyncFuse_GeneratesSource()
    {
        var source = GetNoCompilationSource("MySourcePushAsync");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MySourcePushAsync.g.cs"));
        Assert.NotNull(f);
        var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Source<Steelax.Pufflow.Abstractions.IAsyncProducator<T>>>",
            c);
        Assert.Contains("FlowAProd", c);
        Assert.DoesNotContain("GetFlow", c);
    }
}
