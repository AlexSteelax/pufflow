using static Steelax.Pufflow.Generator.Tests.TestMarshal;

namespace Steelax.Pufflow.Generator.Tests;

public partial class PipeTests
{
    [Fact]
    public void PipePushFuse_GeneratesPipe()
    {
        var source = GetNoCompilationSource("MyPipePush");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyPipePush.g.cs"));
        Assert.NotNull(f);
        var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Pipe<System.Collections.Generic.IEnumerator<T1>, Steelax.Pufflow.Abstractions.IProducator<T2>>>",
            c);
        Assert.Contains("FlowEnumToProd", c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void PipePushAsyncFuse_GeneratesPipe()
    {
        var source = GetNoCompilationSource("MyPipePushAsync");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyPipePushAsync.g.cs"));
        Assert.NotNull(f);
        var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Pipe<System.Collections.Generic.IAsyncEnumerator<T1>, Steelax.Pufflow.Abstractions.IAsyncProducator<T2>>>",
            c);
        Assert.Contains("FlowAEnumToAProd", c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void MultiFuse_GeneratesSingleInterface()
    {
        var source = GetNoCompilationSource("MyMultiExecute");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyMultiExecute.g.cs"));
        Assert.NotNull(f);
        var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Pipe<System.Collections.Generic.IEnumerator<T1>, Steelax.Pufflow.Abstractions.IProducator<T2>>>",
            c);
        Assert.Contains("FlowEnumToProd", c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void PipeConsumatorToProducator_GeneratesSingleVariant()
    {
        var source = GetNoCompilationSource("MyPipeConsumatorToProducator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyPipeConsumatorToProducator.g.cs"));
        Assert.NotNull(f);
        var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.IConsumator<T1>, Steelax.Pufflow.Abstractions.IProducator<T2>>>",
            c);
        Assert.Contains("FlowConsToProd", c);
        Assert.DoesNotContain("Steelax.Pufflow.Abstractions.Sync", c);
        Assert.DoesNotContain("Steelax.Pufflow.Abstractions.Async", c);
    }
}
