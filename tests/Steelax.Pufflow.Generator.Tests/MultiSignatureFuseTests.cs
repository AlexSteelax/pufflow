using static Steelax.Pufflow.Generator.Tests.TestMarshal;

namespace Steelax.Pufflow.Generator.Tests;

public class MultiSignatureFuseTests
{
    /// <summary>
    ///     A single class with multiple <c>Fuse</c> overloads (as the real <c>FlowPipeProducator</c> flow)
    ///     must produce one <c>IFlowable<Pipe<...>></c> marker per signature.
    /// </summary>
    [Fact]
    public void FlowPipeProducator_MultipleFuseSignatures_GeneratesAllPipes()
    {
        var source = GetNoCompilationSource("FlowPipeProducator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("FlowPipeProducator.g.cs"));
        Assert.NotNull(f);
        var c = f.GetText(TestContext.Current.CancellationToken).ToString();

        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Pipe<System.Collections.Generic.IEnumerator<T1>, Steelax.Pufflow.Abstractions.IProducator<T2>>>",
            c);
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Pipe<System.Collections.Generic.IEnumerator<T1>, Steelax.Pufflow.Abstractions.IAsyncProducator<T2>>>",
            c);
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.IConsumator<T1>, Steelax.Pufflow.Abstractions.IProducator<T2>>>",
            c);
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.IConsumator<T1>, Steelax.Pufflow.Abstractions.IAsyncProducator<T2>>>",
            c);
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Pipe<System.Collections.Generic.IAsyncEnumerator<T1>, Steelax.Pufflow.Abstractions.IProducator<T2>>>",
            c);
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Pipe<System.Collections.Generic.IAsyncEnumerator<T1>, Steelax.Pufflow.Abstractions.IAsyncProducator<T2>>>",
            c);
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.IAsyncConsumator<T1>, Steelax.Pufflow.Abstractions.IProducator<T2>>>",
            c);
        Assert.Contains(
            "IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.IAsyncConsumator<T1>, Steelax.Pufflow.Abstractions.IAsyncProducator<T2>>>",
            c);

        Assert.DoesNotContain("Steelax.Pufflow.Abstractions.Sync", c);
        Assert.DoesNotContain("Steelax.Pufflow.Abstractions.Async", c);
        Assert.DoesNotContain("GetFlow", c);
    }

    /// <summary>
    ///     View properties use abbreviated interface names: Enum/AEnum/Cons/ACons/Prod/AProd.
    /// </summary>
    [Fact]
    public void FlowPipeProducator_ViewPropertiesUseAbbreviations()
    {
        var source = GetNoCompilationSource("FlowPipeProducator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("FlowPipeProducator.g.cs"));
        Assert.NotNull(f);
        var c = f.GetText(TestContext.Current.CancellationToken).ToString();

        Assert.Contains("FlowEnumToProd", c);
        Assert.Contains("FlowEnumToAProd", c);
        Assert.Contains("FlowConsToProd", c);
        Assert.Contains("FlowConsToAProd", c);
        Assert.Contains("FlowAEnumToProd", c);
        Assert.Contains("FlowAEnumToAProd", c);
        Assert.Contains("FlowAConsToProd", c);
        Assert.Contains("FlowAConsToAProd", c);

        Assert.DoesNotContain("FlowWithEnumeratorToProducator", c);
        Assert.DoesNotContain("FlowWithAsyncConsumatorToAsyncProducator", c);
    }
}
