using System.Reflection;
using System.Text;
using JetBrains.Annotations;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Generator.Tests;

public static class TestMarshal
{
    [PublicAPI]
    public static string GetNoCompilationSource(string name)
    {
        var assembly = Assembly.GetExecutingAssembly();

        var resourceName = $"{typeof(TestMarshal).Namespace}.NoCompilationSources.{name}.cs";

        using var stream = assembly.GetManifestResourceStream(resourceName);

        if (stream == null)
            throw new FileNotFoundException($"Resource '{resourceName}' not found.");

        using var reader = new StreamReader(stream);

        return reader.ReadToEnd();
    }

    [PublicAPI]
    public static GeneratorDriverRunResult RunGenerator(string sourceCode)
    {
        var generator = new GetFlowGenerator();
        var driver = CSharpGeneratorDriver.Create(generator);

        var compilation = CSharpCompilation.Create("Test",
            [CSharpSyntaxTree.ParseText(sourceCode)],
            [
                MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
                MetadataReference.CreateFromFile(typeof(Enumerable).Assembly.Location),
                MetadataReference.CreateFromFile(typeof(IEnumerator<int>).Assembly.Location),
                MetadataReference.CreateFromFile(typeof(FlowContext).Assembly.Location),
                MetadataReference.CreateFromFile(typeof(FlowAttribute).Assembly.Location)
            ]);

        return driver.RunGenerators(compilation).GetRunResult();
    }

    /// <summary>
    ///     Builds a regex matching the generated static <c>Configure</c> return marker, e.g.
    ///     <c>public static Pipe&lt;IConsumator&lt;T1&gt;, IAsyncProducator&lt;T2&gt;&gt; Configure(</c>.
    /// </summary>
    [PublicAPI]
    public static string ConfigureMarkerPattern(Type kind, params Type[] ifaces)
    {
        var kindName = StripArity(kind.Name);

        var sb = new StringBuilder();
        sb.Append("public static Steelax\\.Pufflow\\.").Append(kindName).Append('<');

        for (var i = 0; i < ifaces.Length; i++)
        {
            if (i != 0)
                sb.Append(", ");

            sb.Append("Steelax\\.Pufflow\\.Abstractions\\.").Append(StripArity(ifaces[i].Name)).Append("<[^>]+>");
        }

        sb.Append("> Configure\\(");

        return sb.ToString();
    }

    private static string StripArity(string name)
    {
        var tick = name.IndexOf('`');
        return tick >= 0 ? name[..tick] : name;
    }
}