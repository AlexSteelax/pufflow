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
    public void NoHandlerMethod_DoesNotGenerate()
    {
        var runResult = RunGenerator(GetNoCompilationSource("NoHandler"));

        Assert.DoesNotContain(runResult.GeneratedTrees, t => t.FilePath.EndsWith("NoHandler.g.cs"));
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
        Assert.DoesNotMatch(@"public partial class MyConstrained<T, TBatch>\s+where", text);

        // The flow marker itself is still produced (a Pipe over async enumerators, like the MyPipe*To* fixtures).
        Assert.Matches(FlowMarkerPattern(typeof(Pipe<,>), typeof(IAsyncEnumerator<>), typeof(IAsyncEnumerator<>)), text);
    }
}
