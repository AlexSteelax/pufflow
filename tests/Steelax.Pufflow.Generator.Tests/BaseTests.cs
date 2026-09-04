using Steelax.Pufflow.Abstractions;
using static Steelax.Pufflow.Generator.Tests.TestMarshal;

namespace Steelax.Pufflow.Generator.Tests;

public class BaseTests
{
    public static TheoryData<string, Type, Type[]> Cases => new()
    {
        // Sources
        { "MySource", typeof(Source<>), [typeof(IEnumerator<>)] },
        { "MyAsyncSource", typeof(Source<>), [typeof(IAsyncEnumerator<>)] },
        { "MyConsumator", typeof(Source<>), [typeof(IConsumator<>)] },
        { "MySourceAsyncConsumator", typeof(Source<>), [typeof(IAsyncConsumator<>)] },
        { "MySourcePush", typeof(Source<>), [typeof(IProducator<>)] },
        { "MySourcePushAsync", typeof(Source<>), [typeof(IAsyncProducator<>)] },

        // Sinks
        { "MySinkPull", typeof(Sink<>), [typeof(IEnumerator<>)] },
        { "MySinkAsyncPull", typeof(Sink<>), [typeof(IAsyncEnumerator<>)] },
        { "MySinkPush", typeof(Sink<>), [typeof(IProducator<>)] },
        { "MySinkConsumator", typeof(Sink<>), [typeof(IConsumator<>)] },
        { "MySinkAsyncConsumator", typeof(Sink<>), [typeof(IAsyncConsumator<>)] },
        { "MySinkPushAsync", typeof(Sink<>), [typeof(IAsyncProducator<>)] },

        // Pipes
        { "MyPipeEnumeratorToEnumerator", typeof(Pipe<,>), [typeof(IEnumerator<>), typeof(IEnumerator<>)] },
        { "MyPipeAsyncEnumeratorToAsyncEnumerator", typeof(Pipe<,>), [typeof(IAsyncEnumerator<>), typeof(IAsyncEnumerator<>)] },
        { "MyPipeEnumeratorToConsumator", typeof(Pipe<,>), [typeof(IEnumerator<>), typeof(IConsumator<>)] },
        { "MyPipeProducatorToProducator", typeof(Pipe<,>), [typeof(IProducator<>), typeof(IProducator<>)] },
        { "MyPipeEnumeratorToProducator", typeof(Pipe<,>), [typeof(IEnumerator<>), typeof(IProducator<>)] },
        { "MyPipeConsumatorToProducator", typeof(Pipe<,>), [typeof(IConsumator<>), typeof(IProducator<>)] },
        { "MyPipeAsyncProducatorToAsyncConsumator", typeof(Pipe<,>), [typeof(IAsyncProducator<>), typeof(IAsyncConsumator<>)] },
        { "MyPipeProducatorToConsumator", typeof(Pipe<,>), [typeof(IProducator<>), typeof(IConsumator<>)] },
        { "MyPipeEnumeratorToAsyncConsumator", typeof(Pipe<,>), [typeof(IEnumerator<>), typeof(IAsyncConsumator<>)] },
        { "MyPipeEnumeratorToAsyncEnumerator", typeof(Pipe<,>), [typeof(IEnumerator<>), typeof(IAsyncEnumerator<>)] },
        { "MyPipeEnumToAsyncProducator", typeof(Pipe<,>), [typeof(IEnumerator<>), typeof(IAsyncProducator<>)] },
        { "MyPipeConsumatorToEnumerator", typeof(Pipe<,>), [typeof(IConsumator<>), typeof(IEnumerator<>)] },
        { "MyPipeConsumatorToAsyncEnumerator", typeof(Pipe<,>), [typeof(IConsumator<>), typeof(IAsyncEnumerator<>)] },
        { "MyPipeConsumatorToConsumator", typeof(Pipe<,>), [typeof(IConsumator<>), typeof(IConsumator<>)] },
        { "MyPipeConsumatorToAsyncConsumator", typeof(Pipe<,>), [typeof(IConsumator<>), typeof(IAsyncConsumator<>)] },
        { "MyPipeConsumatorToAsyncProducator", typeof(Pipe<,>), [typeof(IConsumator<>), typeof(IAsyncProducator<>)] },
        { "MyPipeAsyncEnumToProducator", typeof(Pipe<,>), [typeof(IAsyncEnumerator<>), typeof(IProducator<>)] },
        { "MyPipeAsyncEnumToAsyncProducator", typeof(Pipe<,>), [typeof(IAsyncEnumerator<>), typeof(IAsyncProducator<>)] },
        { "MyPipeAsyncConsumatorToEnumerator", typeof(Pipe<,>), [typeof(IAsyncConsumator<>), typeof(IEnumerator<>)] },
        { "MyPipeAsyncConsumatorToConsumator", typeof(Pipe<,>), [typeof(IAsyncConsumator<>), typeof(IConsumator<>)] },
        { "MyPipeAsyncConsumatorToAsyncConsumator", typeof(Pipe<,>), [typeof(IAsyncConsumator<>), typeof(IAsyncConsumator<>)] },
        { "MyPipeAsyncConsumatorToAsyncEnumerator", typeof(Pipe<,>), [typeof(IAsyncConsumator<>), typeof(IAsyncEnumerator<>)] },
        { "MyPipeAsyncConsumatorToProducator", typeof(Pipe<,>), [typeof(IAsyncConsumator<>), typeof(IProducator<>)] },
        { "MyPipeAsyncConsumatorToAsyncProducator", typeof(Pipe<,>), [typeof(IAsyncConsumator<>), typeof(IAsyncProducator<>)] },
        { "MyPipeProducatorToAsyncProducator", typeof(Pipe<,>), [typeof(IProducator<>), typeof(IAsyncProducator<>)] },
        { "MyPipeAsyncProducatorToProducator", typeof(Pipe<,>), [typeof(IAsyncProducator<>), typeof(IProducator<>)] },
        { "MyPipeAsyncProducatorToAsyncProducator", typeof(Pipe<,>), [typeof(IAsyncProducator<>), typeof(IAsyncProducator<>)] },
        { "MyPipeAsyncEnumToAsyncConsumator", typeof(Pipe<,>), [typeof(IAsyncEnumerator<>), typeof(IAsyncConsumator<>)] },
        { "MyPipeProducatorToAsyncConsumator", typeof(Pipe<,>), [typeof(IProducator<>), typeof(IAsyncConsumator<>)] }
    };

    public static TheoryData<string, Type[]> ViewCases => new (Cases.Select(s => new TheoryDataRow<string, Type[]>(s.Data.Item1, s.Data.Item3))) ;

    [Theory]
    [MemberData(nameof(Cases))]
    public void GeneratesFlowMarker(string name, Type flow, Type[] args)
    {
        var source = GetNoCompilationSource(name);
        var runResult = RunGenerator(source);
        var tree = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith($"{name}.g.cs"));

        Assert.NotNull(tree);

        var text = tree.GetText(TestContext.Current.CancellationToken).ToString();

        Assert.Matches(FlowMarkerPattern(flow, args), text);
    }

    [Theory]
    [MemberData(nameof(ViewCases))]
    public void GeneratesFlowView(string name, Type[] args)
    {
        var source = GetNoCompilationSource(name);
        var runResult = RunGenerator(source);
        var tree = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith($"{name}.g.cs"));

        Assert.NotNull(tree);

        var text = tree.GetText(TestContext.Current.CancellationToken).ToString();

        Assert.Matches(FlowViewPattern(args), text);
    }
}
