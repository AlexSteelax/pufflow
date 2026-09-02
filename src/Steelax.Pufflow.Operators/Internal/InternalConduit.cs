using Steelax.Toolkit.HighPerformance.Concurrency.Collections;

namespace Steelax.Pufflow.Operators.Internal;

internal sealed class InternalConduit<T>(int capacity, ConduitBehavior behavior = ConduitBehavior.Default) :
    Conduit<T>(capacity, behavior),
    IAsyncConsumator<T>,
    IAsyncProducator<T>;