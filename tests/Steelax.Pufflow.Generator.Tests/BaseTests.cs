using Steelax.Pufflow;
using Steelax.Pufflow.Abstractions;
using static Steelax.Pufflow.Generator.Tests.TestMarshal;

namespace Steelax.Pufflow.Generator.Tests;

public class BaseTests
{
    public static TheoryData<string, Type, Type[]> ConfigureCases => new()
    {
        // Sources
        { "MySourceConsumator", typeof(Source<>), [typeof(IConsumator<>)] },
        { "MySourceAsyncConsumator", typeof(Source<>), [typeof(IAsyncConsumator<>)] },
        { "MySourceProducator", typeof(Source<>), [typeof(IProducator<>)] },
        { "MySourceAsyncProducator", typeof(Source<>), [typeof(IAsyncProducator<>)] },

        // Sinks
        { "MySinkConsumator", typeof(Sink<>), [typeof(IConsumator<>)] },
        { "MySinkAsyncConsumator", typeof(Sink<>), [typeof(IAsyncConsumator<>)] },
        { "MySinkProducator", typeof(Sink<>), [typeof(IProducator<>)] },
        { "MySinkAsyncProducator", typeof(Sink<>), [typeof(IAsyncProducator<>)] },

        // Pipes: pull -> pull
        { "MyPipeConsumatorToConsumator", typeof(Pipe<,>), [typeof(IConsumator<>), typeof(IConsumator<>)] },
        { "MyPipeConsumatorToAsyncConsumator", typeof(Pipe<,>), [typeof(IConsumator<>), typeof(IAsyncConsumator<>)] },
        { "MyPipeAsyncConsumatorToConsumator", typeof(Pipe<,>), [typeof(IAsyncConsumator<>), typeof(IConsumator<>)] },
        { "MyPipeAsyncConsumatorToAsyncConsumator", typeof(Pipe<,>), [typeof(IAsyncConsumator<>), typeof(IAsyncConsumator<>)] },

        // Pipes: pull -> push (hybrid)
        { "MyPipeConsumatorToProducator", typeof(Pipe<,>), [typeof(IConsumator<>), typeof(IProducator<>)] },
        { "MyPipeConsumatorToAsyncProducator", typeof(Pipe<,>), [typeof(IConsumator<>), typeof(IAsyncProducator<>)] },
        { "MyPipeAsyncConsumatorToProducator", typeof(Pipe<,>), [typeof(IAsyncConsumator<>), typeof(IProducator<>)] },
        { "MyPipeAsyncConsumatorToAsyncProducator", typeof(Pipe<,>), [typeof(IAsyncConsumator<>), typeof(IAsyncProducator<>)] },

        // Pipes: push -> push
        { "MyPipeProducatorToProducator", typeof(Pipe<,>), [typeof(IProducator<>), typeof(IProducator<>)] },
        { "MyPipeProducatorToAsyncProducator", typeof(Pipe<,>), [typeof(IProducator<>), typeof(IAsyncProducator<>)] },
        { "MyPipeAsyncProducatorToProducator", typeof(Pipe<,>), [typeof(IAsyncProducator<>), typeof(IProducator<>)] },
        { "MyPipeAsyncProducatorToAsyncProducator", typeof(Pipe<,>), [typeof(IAsyncProducator<>), typeof(IAsyncProducator<>)] },

        // Pipes: push -> pull (composite)
        { "MyPipeProducatorToConsumator", typeof(Pipe<,>), [typeof(IProducator<>), typeof(IConsumator<>)] },
        { "MyPipeProducatorToAsyncConsumator", typeof(Pipe<,>), [typeof(IProducator<>), typeof(IAsyncConsumator<>)] },
        { "MyPipeAsyncProducatorToConsumator", typeof(Pipe<,>), [typeof(IAsyncProducator<>), typeof(IConsumator<>)] },
        { "MyPipeAsyncProducatorToAsyncConsumator", typeof(Pipe<,>), [typeof(IAsyncProducator<>), typeof(IAsyncConsumator<>)] }
    };

    [Theory]
    [MemberData(nameof(ConfigureCases))]
    public void GeneratesConfigureMarker(string name, Type kind, Type[] ifaces)
    {
        var source = GetNoCompilationSource(name);
        var runResult = RunGenerator(source);
        var tree = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith($"{name}.g.cs"));

        Assert.NotNull(tree);

        var text = tree.GetText(TestContext.Current.CancellationToken).ToString();

        Assert.Matches(ConfigureMarkerPattern(kind, ifaces), text);
        Assert.Contains("Configure(", text);
    }
}