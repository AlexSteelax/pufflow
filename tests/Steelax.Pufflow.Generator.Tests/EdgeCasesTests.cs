using static Steelax.Pufflow.Generator.Tests.TestMarshal;

namespace Steelax.Pufflow.Generator.Tests;

public class EdgeCasesTests
{
    [Fact]
    public void WithoutDataflowAttribute_DoesNotGenerate()
    {
        var source = GetNoCompilationSource("NoAttribute");
        var runResult = RunGenerator(source);
        Assert.DoesNotContain(runResult.GeneratedTrees, t => t.FilePath.EndsWith("NoAttribute.g.cs"));
    }

    [Fact]
    public void NoHandlerMethod_DoesNotGenerate()
    {
        var source = GetNoCompilationSource("NoHandler");
        var runResult = RunGenerator(source);
        Assert.DoesNotContain(runResult.GeneratedTrees, t => t.FilePath.EndsWith("NoHandler.g.cs"));
    }

    [Fact]
    public void GenericClassWithConstraints_DoesNotEmitConstraintsInGeneratedCode()
    {
        var source = GetNoCompilationSource("MyConstrained");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyConstrained.g.cs"));
        Assert.NotNull(f);
        var c = f.GetText(TestContext.Current.CancellationToken).ToString();

        Assert.Contains("public partial class MyConstrained<T, TBatch>", c);
        Assert.DoesNotContain("where", c);
    }
}
