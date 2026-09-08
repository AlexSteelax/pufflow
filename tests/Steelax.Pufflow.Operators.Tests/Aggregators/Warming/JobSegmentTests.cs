namespace Steelax.Pufflow.Operators.Tests.Aggregators.Warming;

/// <summary>
///     Unit tests for the <see cref="Steelax.Pufflow.Operators.Aggregators.Warming.JobSegment{TKey,TWarm}" /> class: key buffering, the result holder
///     contract (warm data vs. no data) and the reuse/apply lifecycle. The concrete test cases live in
///     grouped partial files (<c>ResultApplication</c>, <c>State</c>, <c>Advance</c>, <c>Lifecycle</c>);
///     shared test doubles come from <see cref="WarmingHelper" />.
/// </summary>
public static partial class JobSegmentTests;