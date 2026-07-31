using System.Linq;
using static Steelax.Pufflow.Generator.Tests.TestMarshal;

namespace Steelax.Pufflow.Generator.Tests;

public class GetFlowGeneratorTests
{
    [Fact]
    public void SourceEnumerator_GeneratesFlowInterface()
    {
        var source = GetNoCompilationSource("MySource");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MySource.g.cs"));
        Assert.NotNull(f);
        var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains("IFlowable<Steelax.Pufflow.Source<System.Collections.Generic.IEnumerator<T>>>", c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void PipeEnumerator_GeneratesFlowInterface()
    {
        var source = GetNoCompilationSource("MyTransform");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyTransform.g.cs"));
        Assert.NotNull(f); var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains("IFlowable<Steelax.Pufflow.Pipe<System.Collections.Generic.IEnumerator<T1>, System.Collections.Generic.IEnumerator<T2>>>", c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void PipeAsyncEnumerator_GeneratesFlowInterface()
    {
        var source = GetNoCompilationSource("MyAsyncTransform");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyAsyncTransform.g.cs"));
        Assert.NotNull(f); var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains("IFlowable<Steelax.Pufflow.Pipe<System.Collections.Generic.IAsyncEnumerator<T1>, System.Collections.Generic.IAsyncEnumerator<T2>>>", c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void SourceAsyncEnumerator_GeneratesFlowInterface()
    {
        var source = GetNoCompilationSource("MyAsyncSource");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyAsyncSource.g.cs"));
        Assert.NotNull(f); var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains("IFlowable<Steelax.Pufflow.Source<System.Collections.Generic.IAsyncEnumerator<T>>>", c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void Consumator_GeneratesFlowInterface()
    {
        var source = GetNoCompilationSource("MyConsumator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyConsumator.g.cs"));
        Assert.NotNull(f); var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains("IFlowable<Steelax.Pufflow.Source<Steelax.Pufflow.Abstractions.IConsumator<T>>>", c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void SourcePushExecute_GeneratesSourceWithSync()
    {
        var source = GetNoCompilationSource("MySourcePush");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MySourcePush.g.cs"));
        Assert.NotNull(f); var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains("IFlowable<Steelax.Pufflow.Source<Steelax.Pufflow.Abstractions.Sync, Steelax.Pufflow.Abstractions.IProducator<T>>>", c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void SourcePushAsyncExecute_GeneratesSourceWithAsync()
    {
        var source = GetNoCompilationSource("MySourcePushAsync");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MySourcePushAsync.g.cs"));
        Assert.NotNull(f); var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains("IFlowable<Steelax.Pufflow.Source<Steelax.Pufflow.Abstractions.Async, Steelax.Pufflow.Abstractions.IAsyncProducator<T>>>", c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void SinkPullExecute_GeneratesSinkWithSync()
    {
        var source = GetNoCompilationSource("MySinkPull");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MySinkPull.g.cs"));
        Assert.NotNull(f); var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains("IFlowable<Steelax.Pufflow.Sink<Steelax.Pufflow.Abstractions.Sync, System.Collections.Generic.IEnumerator<T>>>", c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void SinkPushGetProducator_GeneratesSink()
    {
        var source = GetNoCompilationSource("MySinkPush");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MySinkPush.g.cs"));
        Assert.NotNull(f); var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains("IFlowable<Steelax.Pufflow.Sink<Steelax.Pufflow.Abstractions.IProducator<T>>>", c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void PipePushExecute_GeneratesPipeWithSync()
    {
        var source = GetNoCompilationSource("MyPipePush");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyPipePush.g.cs"));
        Assert.NotNull(f); var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains("IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.Sync, System.Collections.Generic.IEnumerator<T1>, Steelax.Pufflow.Abstractions.IProducator<T2>>>", c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void PipeConsumator_GeneratesPipe()
    {
        var source = GetNoCompilationSource("MyPipeConsumator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyPipeConsumator.g.cs"));
        Assert.NotNull(f); var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains("IFlowable<Steelax.Pufflow.Pipe<System.Collections.Generic.IEnumerator<T1>, Steelax.Pufflow.Abstractions.IConsumator<T2>>>", c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void Composite_GeneratesMultipleInterfaces()
    {
        var source = GetNoCompilationSource("MyComposite");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyComposite.g.cs"));
        Assert.NotNull(f); var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains("IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.IAsyncProducator<T1>, Steelax.Pufflow.Abstractions.IAsyncConsumator<T2>>>", c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void SinkAsyncPullExecute_GeneratesSinkWithAsync()
    {
        var source = GetNoCompilationSource("MySinkAsyncPull");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MySinkAsyncPull.g.cs"));
        Assert.NotNull(f); var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains("IFlowable<Steelax.Pufflow.Sink<Steelax.Pufflow.Abstractions.Async, System.Collections.Generic.IAsyncEnumerator<T>>>", c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void PipePushAsyncExecute_GeneratesPipeWithAsync()
    {
        var source = GetNoCompilationSource("MyPipePushAsync");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyPipePushAsync.g.cs"));
        Assert.NotNull(f); var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains("IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.Async, System.Collections.Generic.IAsyncEnumerator<T1>, Steelax.Pufflow.Abstractions.IAsyncProducator<T2>>>", c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void PipeProducatorToProducator_GeneratesPipe()
    {
        var source = GetNoCompilationSource("MyPipeProducator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyPipeProducator.g.cs"));
        Assert.NotNull(f); var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains("IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.IProducator<T1>, Steelax.Pufflow.Abstractions.IProducator<T2>>>", c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void MultiExecute_GeneratesBothInterfaces()
    {
        var source = GetNoCompilationSource("MyMultiExecute");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyMultiExecute.g.cs"));
        Assert.NotNull(f); var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains("IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.Sync, System.Collections.Generic.IEnumerator<T1>, Steelax.Pufflow.Abstractions.IProducator<T2>>>", c);
        Assert.Contains("IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.Async, System.Collections.Generic.IEnumerator<T1>, Steelax.Pufflow.Abstractions.IProducator<T2>>>", c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void SourceAsyncConsumator_GeneratesFlowInterface()
    {
        var source = GetNoCompilationSource("MySourceAsyncConsumator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MySourceAsyncConsumator.g.cs"));
        Assert.NotNull(f); var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains("IFlowable<Steelax.Pufflow.Source<Steelax.Pufflow.Abstractions.IAsyncConsumator<T>>>", c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void SinkConsumatorExecute_GeneratesSinkWithSync()
    {
        var source = GetNoCompilationSource("MySinkConsumator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MySinkConsumator.g.cs"));
        Assert.NotNull(f); var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains("IFlowable<Steelax.Pufflow.Sink<Steelax.Pufflow.Abstractions.Sync, Steelax.Pufflow.Abstractions.IConsumator<T>>>", c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void PipeEnumeratorToAsyncConsumator_GeneratesPipe()
    {
        var source = GetNoCompilationSource("MyPipeEnumeratorToAsyncConsumator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyPipeEnumeratorToAsyncConsumator.g.cs"));
        Assert.NotNull(f); var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains("IFlowable<Steelax.Pufflow.Pipe<System.Collections.Generic.IEnumerator<T1>, Steelax.Pufflow.Abstractions.IAsyncConsumator<T2>>>", c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void PipeConsumatorToEnumerator_GeneratesPipe()
    {
        var source = GetNoCompilationSource("MyPipeConsumatorToEnumerator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyPipeConsumatorToEnumerator.g.cs"));
        Assert.NotNull(f); var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains("IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.IConsumator<T1>, System.Collections.Generic.IEnumerator<T2>>>", c);
        Assert.DoesNotContain("GetFlow", c);
    }

    [Fact]
    public void PipeEnumeratorToAsyncEnumerator_GeneratesPipe()
    {
        var source = GetNoCompilationSource("MyPipeEnumeratorToAsyncEnumerator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyPipeEnumeratorToAsyncEnumerator.g.cs"));
        Assert.NotNull(f);
        Assert.Contains("IFlowable<Steelax.Pufflow.Pipe<System.Collections.Generic.IEnumerator<T1>, System.Collections.Generic.IAsyncEnumerator<T2>>>", f.GetText(TestContext.Current.CancellationToken).ToString());
    }

    [Fact]
    public void PipeConsumatorToConsumator_GeneratesPipe()
    {
        var source = GetNoCompilationSource("MyPipeConsumatorToConsumator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyPipeConsumatorToConsumator.g.cs"));
        Assert.NotNull(f);
        Assert.Contains("IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.IConsumator<T1>, Steelax.Pufflow.Abstractions.IConsumator<T2>>>", f.GetText(TestContext.Current.CancellationToken).ToString());
    }

    [Fact]
    public void PipeConsumatorToAsyncConsumator_GeneratesPipe()
    {
        var source = GetNoCompilationSource("MyPipeConsumatorToAsyncConsumator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyPipeConsumatorToAsyncConsumator.g.cs"));
        Assert.NotNull(f);
        Assert.Contains("IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.IConsumator<T1>, Steelax.Pufflow.Abstractions.IAsyncConsumator<T2>>>", f.GetText(TestContext.Current.CancellationToken).ToString());
    }

    [Fact]
    public void PipeConsumatorToAsyncEnumerator_GeneratesPipe()
    {
        var source = GetNoCompilationSource("MyPipeConsumatorToAsyncEnumerator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyPipeConsumatorToAsyncEnumerator.g.cs"));
        Assert.NotNull(f);
        Assert.Contains("IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.IConsumator<T1>, System.Collections.Generic.IAsyncEnumerator<T2>>>", f.GetText(TestContext.Current.CancellationToken).ToString());
    }

    [Fact]
    public void PipeAsyncConsumatorToAsyncConsumator_GeneratesPipe()
    {
        var source = GetNoCompilationSource("MyPipeAsyncConsumatorToAsyncConsumator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyPipeAsyncConsumatorToAsyncConsumator.g.cs"));
        Assert.NotNull(f);
        Assert.Contains("IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.IAsyncConsumator<T1>, Steelax.Pufflow.Abstractions.IAsyncConsumator<T2>>>", f.GetText(TestContext.Current.CancellationToken).ToString());
    }

    [Fact]
    public void PipeAsyncConsumatorToAsyncEnumerator_GeneratesPipe()
    {
        var source = GetNoCompilationSource("MyPipeAsyncConsumatorToAsyncEnumerator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyPipeAsyncConsumatorToAsyncEnumerator.g.cs"));
        Assert.NotNull(f);
        Assert.Contains("IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.IAsyncConsumator<T1>, System.Collections.Generic.IAsyncEnumerator<T2>>>", f.GetText(TestContext.Current.CancellationToken).ToString());
    }

    [Fact]
    public void PipeAsyncConsumatorToConsumator_GeneratesPipe()
    {
        var source = GetNoCompilationSource("MyPipeAsyncConsumatorToConsumator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyPipeAsyncConsumatorToConsumator.g.cs"));
        Assert.NotNull(f);
        Assert.Contains("IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.IAsyncConsumator<T1>, Steelax.Pufflow.Abstractions.IConsumator<T2>>>", f.GetText(TestContext.Current.CancellationToken).ToString());
    }

    [Fact]
    public void PipeAsyncConsumatorToEnumerator_GeneratesPipe()
    {
        var source = GetNoCompilationSource("MyPipeAsyncConsumatorToEnumerator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyPipeAsyncConsumatorToEnumerator.g.cs"));
        Assert.NotNull(f);
        Assert.Contains("IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.IAsyncConsumator<T1>, System.Collections.Generic.IEnumerator<T2>>>", f.GetText(TestContext.Current.CancellationToken).ToString());
    }

    [Fact]
    public void SinkAsyncConsumatorExecute_GeneratesSinkWithAsync()
    {
        var source = GetNoCompilationSource("MySinkAsyncConsumator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MySinkAsyncConsumator.g.cs"));
        Assert.NotNull(f);
        Assert.Contains("IFlowable<Steelax.Pufflow.Sink<Steelax.Pufflow.Abstractions.Async, Steelax.Pufflow.Abstractions.IAsyncConsumator<T>>>", f.GetText(TestContext.Current.CancellationToken).ToString());
    }

    [Fact]
    public void PipeEnumToAsyncProducator_GeneratesPipeWithAsync()
    {
        var source = GetNoCompilationSource("MyPipeEnumToAsyncProducator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyPipeEnumToAsyncProducator.g.cs"));
        Assert.NotNull(f);
        Assert.Contains("IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.Async, System.Collections.Generic.IEnumerator<T1>, Steelax.Pufflow.Abstractions.IAsyncProducator<T2>>>", f.GetText(TestContext.Current.CancellationToken).ToString());
    }

    [Fact]
    public void PipeAsyncEnumToAsyncProducator_GeneratesPipeWithAsync()
    {
        var source = GetNoCompilationSource("MyPipeAsyncEnumToAsyncProducator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyPipeAsyncEnumToAsyncProducator.g.cs"));
        Assert.NotNull(f);
        Assert.Contains("IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.Async, System.Collections.Generic.IAsyncEnumerator<T1>, Steelax.Pufflow.Abstractions.IAsyncProducator<T2>>>", f.GetText(TestContext.Current.CancellationToken).ToString());
    }

    [Fact]
    public void PipeAsyncEnumToProducator_GeneratesPipeWithAsync()
    {
        var source = GetNoCompilationSource("MyPipeAsyncEnumToProducator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyPipeAsyncEnumToProducator.g.cs"));
        Assert.NotNull(f);
        Assert.Contains("IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.Async, System.Collections.Generic.IAsyncEnumerator<T1>, Steelax.Pufflow.Abstractions.IProducator<T2>>>", f.GetText(TestContext.Current.CancellationToken).ToString());
    }

    [Fact]
    public void PipeConsumatorToAsyncProducator_GeneratesPipeWithAsync()
    {
        var source = GetNoCompilationSource("MyPipeConsumatorToAsyncProducator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyPipeConsumatorToAsyncProducator.g.cs"));
        Assert.NotNull(f);
        Assert.Contains("IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.Async, Steelax.Pufflow.Abstractions.IConsumator<T1>, Steelax.Pufflow.Abstractions.IAsyncProducator<T2>>>", f.GetText(TestContext.Current.CancellationToken).ToString());
    }

    [Fact]
    public void PipeAsyncConsumatorToProducator_GeneratesPipeWithAsync()
    {
        var source = GetNoCompilationSource("MyPipeAsyncConsumatorToProducator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyPipeAsyncConsumatorToProducator.g.cs"));
        Assert.NotNull(f);
        Assert.Contains("IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.Async, Steelax.Pufflow.Abstractions.IAsyncConsumator<T1>, Steelax.Pufflow.Abstractions.IProducator<T2>>>", f.GetText(TestContext.Current.CancellationToken).ToString());
    }

    [Fact]
    public void PipeAsyncConsumatorToAsyncProducator_GeneratesPipeWithAsync()
    {
        var source = GetNoCompilationSource("MyPipeAsyncConsumatorToAsyncProducator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyPipeAsyncConsumatorToAsyncProducator.g.cs"));
        Assert.NotNull(f);
        Assert.Contains("IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.Async, Steelax.Pufflow.Abstractions.IAsyncConsumator<T1>, Steelax.Pufflow.Abstractions.IAsyncProducator<T2>>>", f.GetText(TestContext.Current.CancellationToken).ToString());
    }

    [Fact]
    public void PipeConsumatorToProducator_GeneratesBothVariants()
    {
        var source = GetNoCompilationSource("MyPipeConsumatorToProducator");
        var runResult = RunGenerator(source);
        var f = runResult.GeneratedTrees.FirstOrDefault(t => t.FilePath.EndsWith("MyPipeConsumatorToProducator.g.cs"));
        Assert.NotNull(f);
        var c = f.GetText(TestContext.Current.CancellationToken).ToString();
        Assert.Contains("IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.Sync, Steelax.Pufflow.Abstractions.IConsumator<T1>, Steelax.Pufflow.Abstractions.IProducator<T2>>>", c);
        Assert.Contains("IFlowable<Steelax.Pufflow.Pipe<Steelax.Pufflow.Abstractions.Async, Steelax.Pufflow.Abstractions.IConsumator<T1>, Steelax.Pufflow.Abstractions.IProducator<T2>>>", c);
    }

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
