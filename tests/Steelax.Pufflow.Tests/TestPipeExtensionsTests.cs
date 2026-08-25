using System.Threading.Channels;
using Steelax.Pufflow.Sdk.Test;

namespace Steelax.Pufflow.Tests;

/// <summary>
///     Black-box tests for the Test.Sdk pipe extension methods (<c>ToAsyncProducator</c>,
///     <c>ToProducator</c>, <c>ToAsyncConsumator</c>, <c>ToConsumator</c>) applied to every source
///     family (sync/async producator and sync/async consumator). One partial file per extension method.
/// </summary>
public partial class TestPipeExtensionsTests
{
    private const int TimeoutMs = 1_000;

    private static int TimesTen(int value) => value * 10;

    private static readonly int[] Input = [1, 2, 3];

    private static readonly int[] Expected = [10, 20, 30];

    private static async Task<List<int>> DrainAsync(FlowSource flow, ChannelReader<int> reader, CancellationToken cancellationToken)
    {
        await flow.ExecuteAsync(cancellationToken);
        return await reader.ReadAllAsync(TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);
    }
}
