using Steelax.Toolkit.HighPerformance.Concurrency;

namespace Steelax.Pufflow.Operators;

internal class InternalEventQueue<T>(int capacity) : EventQueue<T>(capacity, true), IAsyncConsumator<T>;