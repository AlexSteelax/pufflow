using System.Reflection;
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
}