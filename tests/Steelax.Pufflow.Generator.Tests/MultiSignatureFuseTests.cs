using Steelax.Pufflow.Abstractions;
using static Steelax.Pufflow.Generator.Tests.TestMarshal;

namespace Steelax.Pufflow.Generator.Tests;

/// <summary>
///     A single class with multiple <c>Fuse</c> overloads (as the real <c>FlowPipeProducator</c> flow)
///     must produce one marker and one disambiguation view per signature, without legacy naming.
/// </summary>
public class MultiSignatureFuseTests
{
    [Theory]
    [InlineData(typeof(IEnumerator<>), typeof(IProducator<>))]
    [InlineData(typeof(IEnumerator<>), typeof(IAsyncProducator<>))]
    [InlineData(typeof(IConsumator<>), typeof(IProducator<>))]
    [InlineData(typeof(IConsumator<>), typeof(IAsyncProducator<>))]
    [InlineData(typeof(IAsyncEnumerator<>), typeof(IProducator<>))]
    [InlineData(typeof(IAsyncEnumerator<>), typeof(IAsyncProducator<>))]
    [InlineData(typeof(IAsyncConsumator<>), typeof(IProducator<>))]
    [InlineData(typeof(IAsyncConsumator<>), typeof(IAsyncProducator<>))]
    public void FlowPipeProducator_GeneratesMarkerAndViewPerSignature(Type left, Type right)
    {
        var source = GetNoCompilationSource("FlowPipeProducator");
        var runResult = RunGenerator(source);
        var tree = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("FlowPipeProducator.g.cs"));

        Assert.NotNull(tree);

        var text = tree.GetText(TestContext.Current.CancellationToken).ToString();

        Assert.Matches(FlowMarkerPattern(typeof(Pipe<,>), left, right), text);
        Assert.Matches(FlowViewPattern(left, right), text);
    }
}
