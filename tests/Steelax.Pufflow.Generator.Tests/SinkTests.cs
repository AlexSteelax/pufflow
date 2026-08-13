using static Steelax.Pufflow.Generator.Tests.TestMarshal;

namespace Steelax.Pufflow.Generator.Tests;

public partial class SinkTests
{
    [Fact]
    public void SinkPushGetProducator_GeneratesSink()
    {
        var source = GetNoCompilationSource("MySinkPush");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MySinkPush.g.cs"));
        Assert.NotNull(f);
        var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains("IFlowable<Steelax.Pufflow.Sink<Steelax.Pufflow.Abstractions.IProducator<T>>>", c);
        Assert.Contains("FlowProd", c);
        Assert.DoesNotContain("GetFlow", c);
    }
}
