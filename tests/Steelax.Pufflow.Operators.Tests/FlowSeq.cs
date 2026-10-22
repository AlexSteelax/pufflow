using JetBrains.Annotations;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Tests;

[PublicAPI]
public static class FlowSeq
{
    /// <summary>
    ///     Builds a carrier stream from explicit <c>(value, watermark)</c> pairs, making the progress attached to
    ///     every item visible in the test.
    /// </summary>
    /// <param name="values">The <c>(item, raw watermark)</c> pairs to wrap into carriers.</param>
    public static List<Carrier<int>> SeqValues(params IEnumerable<(int Item, long Watermark)> values) =>
        values.Select(static v => new Carrier<int>(v.Item, Watermark.From(v.Watermark))).ToList();
    
    public static List<Carrier<int>> SeqValuesNoWatermark(IEnumerable<int> values) =>
        values.Select(static v => new Carrier<int>(v, Watermark.Nothing)).ToList();
}