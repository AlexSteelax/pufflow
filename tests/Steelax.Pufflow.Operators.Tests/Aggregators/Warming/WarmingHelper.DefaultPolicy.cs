using JetBrains.Annotations;
using Steelax.Pufflow.Operators.Aggregators.Warming;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Warming;

internal static partial class WarmingHelper
{
    /// <summary>A sink collecting warm results in emission order.</summary>
    [PublicAPI]
    public sealed class DefaultPolicy : IWarmPolicy<int, string>
    {
        private Dictionary<int, (string Result, bool HasWarm)> _items = [];

        public bool ShouldWarm(int key) => !_items.ContainsKey(key);

        public void OnWarmed(int key, string warm) => _items.Add(key, (warm, true));

        public void OnWarmed(int key) => _items.Add(key, (string.Empty, false));
        
        public IReadOnlyDictionary<int, (string Result, bool HasWarm)> Items => _items;
        public IReadOnlyList<(int Key, string Result, bool HasWarm)> PlainItems => _items
            .Select(static kv => (kv.Key, kv.Value.Item1, kv.Value.Item2))
            .OrderBy(static v => v.Key)
            .ToList();
    }
}