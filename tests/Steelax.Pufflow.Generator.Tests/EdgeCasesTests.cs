using Steelax.Pufflow;
using Steelax.Pufflow.Abstractions;
using static Steelax.Pufflow.Generator.Tests.TestMarshal;

namespace Steelax.Pufflow.Generator.Tests;

public class EdgeCasesTests
{
    [Fact]
    public void WithoutDataflowAttribute_DoesNotGenerate()
    {
        var runResult = RunGenerator(GetNoCompilationSource("NoAttribute"));

        Assert.DoesNotContain(runResult.GeneratedTrees, t => t.FilePath.EndsWith("NoAttribute.g.cs"));
    }

    [Fact]
    public void NoFlowConstructor_DoesNotGenerate()
    {
        var runResult = RunGenerator(GetNoCompilationSource("NoFlowConstructor"));

        Assert.DoesNotContain(runResult.GeneratedTrees, t => t.FilePath.EndsWith("NoFlowConstructor.g.cs"));
    }

    [Fact]
    public void GenericClassWithConstraints_DoesNotEmitConstraintsInGeneratedCode()
    {
        var source = GetNoCompilationSource("MyConstrained");
        var runResult = RunGenerator(source);
        var tree = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyConstrained.g.cs"));

        Assert.NotNull(tree);

        var text = tree.GetText(TestContext.Current.CancellationToken).ToString();

        // Constraints are declared on the user-authored partial, never on the emitted generic args.
        Assert.DoesNotMatch(@"partial class MyConstrained<T, TBatch>\s+where", text);

        // The Configure factory is still produced (a pipe over async consumators).
        Assert.Matches(ConfigureMarkerPattern(typeof(Pipe<,>), typeof(IAsyncConsumator<>), typeof(IAsyncConsumator<>)), text);
    }
}