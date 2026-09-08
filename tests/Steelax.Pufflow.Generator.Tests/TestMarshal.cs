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

    [PublicAPI]
    public static string FlowMarkerPattern(Type kind, params Type?[] args)
    {
        if (kind == typeof(Source<>) || kind == typeof(Sink<>))
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(args.Length, 1);
            
            var k = kind.FullName.TrimEnd("`1");
            var s = args[0]!.FullName.TrimEnd("`1");

            return $"{k}<{s}<.+>>";
        }

        if (kind == typeof(Pipe<,>))
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(args.Length, 2);
            
            var k = kind.FullName.TrimEnd("`2");
            var i = args[0]!.FullName.TrimEnd("`1");
            var o = args[1]!.FullName.TrimEnd("`1");
            
            return @$"{k}<{i}<.+>,\s?{o}<.+>>";
        }
        
        throw new InvalidOperationException("Unexpected kind: " + kind);
    }

    [PublicAPI]
    public static string FlowViewPattern(params Type?[] args)
    {
        ArgumentOutOfRangeException.ThrowIfZero(args.Length);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(args.Length, 2);

        var sb = new StringBuilder();

        sb.Append("Flow");

        for (var i = 0; i < args.Length; i++)
        {
            if (i != 0)
                sb.Append("To");
            
            var arg = args[i];
            
            if (arg == typeof(IProducator<>))
                sb.Append("Prod");
            else if (arg == typeof(IAsyncProducator<>))
                sb.Append("AProd");
            else if (arg == typeof(IConsumator<>))
                sb.Append("Cons");
            else if (arg == typeof(IAsyncConsumator<>))
                sb.Append("ACons");
            else if (arg == typeof(IEnumerator<>))
                sb.Append("Enum");
            else if (arg == typeof(IAsyncEnumerator<>))
                sb.Append("AEnum");
        }
        
        return sb.ToString();
    }
}