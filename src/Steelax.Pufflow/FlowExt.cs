using System.Collections.Generic;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow;

/// <summary>
///     Provides extension methods for composing <c>[Flow]</c> blocks into a pipeline: opening a source
///     (<c>On</c>), chaining a pipe (<c>Next</c>) and terminating with a sink (<c>End</c>). Composition is
///     lazy — the concrete block instances are constructed when the chain is built at <c>End</c>.
/// </summary>
[PublicAPI]
public static class FlowExt
{
    #region On

    /// <summary>Opens a chain from a synchronous pull source.</summary>
    [PublicAPI]
    public static Fuse<IConsumator<T>> On<T>(this FlowSource source, Source<IConsumator<T>> right)
    {
        return new Fuse<IConsumator<T>>(source, right.Invoke());
    }

    /// <summary>Opens a chain from an asynchronous pull source.</summary>
    [PublicAPI]
    public static Fuse<IAsyncConsumator<T>> On<T>(this FlowSource source, Source<IAsyncConsumator<T>> right)
    {
        return new Fuse<IAsyncConsumator<T>>(source, right.Invoke());
    }

    /// <summary>Opens a chain from a synchronous push source.</summary>
    [PublicAPI]
    public static Fuse<IProducator<T>> On<T>(this FlowSource source, Source<IProducator<T>> right)
    {
        return new Fuse<IProducator<T>>(source, right.Invoke());
    }

    /// <summary>Opens a chain from an asynchronous push source.</summary>
    [PublicAPI]
    public static Fuse<IAsyncProducator<T>> On<T>(this FlowSource source, Source<IAsyncProducator<T>> right)
    {
        return new Fuse<IAsyncProducator<T>>(source, right.Invoke());
    }

    #endregion

    #region Next: sync pull input

    /// <summary>Chains a pipe consuming a synchronous pull reader.</summary>
    [PublicAPI]
    public static Fuse<IConsumator<T2>> Next<T1, T2>(this Fuse<IConsumator<T1>> left, Pipe<IConsumator<T1>, IConsumator<T2>> right)
    {
        return new Fuse<IConsumator<T2>>(left.FlowSource, right.Invoke(left.Next));
    }

    /// <summary>Chains a pipe consuming a synchronous pull reader.</summary>
    [PublicAPI]
    public static Fuse<IAsyncConsumator<T2>> Next<T1, T2>(this Fuse<IConsumator<T1>> left, Pipe<IConsumator<T1>, IAsyncConsumator<T2>> right)
    {
        return new Fuse<IAsyncConsumator<T2>>(left.FlowSource, right.Invoke(left.Next));
    }

    /// <summary>Chains a hybrid pipe: reads a synchronous pull stream and pushes into the target.</summary>
    [PublicAPI]
    public static Fuse<IProducator<T2>> Next<T1, T2>(this Fuse<IConsumator<T1>> left, Pipe<IConsumator<T1>, IProducator<T2>> right)
    {
        return new Fuse<IProducator<T2>>(left.FlowSource, right.Invoke(left.Next));
    }

    /// <summary>Chains a hybrid pipe: reads a synchronous pull stream and pushes into the async target.</summary>
    [PublicAPI]
    public static Fuse<IAsyncProducator<T2>> Next<T1, T2>(this Fuse<IConsumator<T1>> left, Pipe<IConsumator<T1>, IAsyncProducator<T2>> right)
    {
        return new Fuse<IAsyncProducator<T2>>(left.FlowSource, right.Invoke(left.Next));
    }

    /// <summary>Chains a pipe consuming a synchronous pull reader (an async upstream feeds it via upcast).</summary>
    [PublicAPI]
    public static Fuse<IConsumator<T2>> Next<T1, T2>(this Fuse<IAsyncConsumator<T1>> left, Pipe<IConsumator<T1>, IConsumator<T2>> right)
    {
        return new Fuse<IConsumator<T2>>(left.FlowSource, right.Invoke(left.Next));
    }

    /// <summary>Chains a pipe consuming a synchronous pull reader (an async upstream feeds it via upcast).</summary>
    [PublicAPI]
    public static Fuse<IAsyncConsumator<T2>> Next<T1, T2>(this Fuse<IAsyncConsumator<T1>> left, Pipe<IConsumator<T1>, IAsyncConsumator<T2>> right)
    {
        return new Fuse<IAsyncConsumator<T2>>(left.FlowSource, right.Invoke(left.Next));
    }

    /// <summary>Chains a hybrid pipe: reads a synchronous pull stream and pushes into the target (an async upstream feeds it via upcast).</summary>
    [PublicAPI]
    public static Fuse<IProducator<T2>> Next<T1, T2>(this Fuse<IAsyncConsumator<T1>> left, Pipe<IConsumator<T1>, IProducator<T2>> right)
    {
        return new Fuse<IProducator<T2>>(left.FlowSource, right.Invoke(left.Next));
    }

    /// <summary>Chains a hybrid pipe: reads a synchronous pull stream and pushes into the async target (an async upstream feeds it via upcast).</summary>
    [PublicAPI]
    public static Fuse<IAsyncProducator<T2>> Next<T1, T2>(this Fuse<IAsyncConsumator<T1>> left, Pipe<IConsumator<T1>, IAsyncProducator<T2>> right)
    {
        return new Fuse<IAsyncProducator<T2>>(left.FlowSource, right.Invoke(left.Next));
    }

    #endregion

    #region Next: async pull input

    /// <summary>Chains a pipe consuming an asynchronous pull reader.</summary>
    [PublicAPI]
    public static Fuse<IConsumator<T2>> Next<T1, T2>(this Fuse<IAsyncConsumator<T1>> left, Pipe<IAsyncConsumator<T1>, IConsumator<T2>> right)
    {
        return new Fuse<IConsumator<T2>>(left.FlowSource, right.Invoke(left.Next));
    }

    /// <summary>Chains a pipe consuming an asynchronous pull reader.</summary>
    [PublicAPI]
    public static Fuse<IAsyncConsumator<T2>> Next<T1, T2>(this Fuse<IAsyncConsumator<T1>> left, Pipe<IAsyncConsumator<T1>, IAsyncConsumator<T2>> right)
    {
        return new Fuse<IAsyncConsumator<T2>>(left.FlowSource, right.Invoke(left.Next));
    }

    /// <summary>Chains a hybrid pipe: reads an asynchronous pull stream and pushes into the target.</summary>
    [PublicAPI]
    public static Fuse<IProducator<T2>> Next<T1, T2>(this Fuse<IAsyncConsumator<T1>> left, Pipe<IAsyncConsumator<T1>, IProducator<T2>> right)
    {
        return new Fuse<IProducator<T2>>(left.FlowSource, right.Invoke(left.Next));
    }

    /// <summary>Chains a hybrid pipe: reads an asynchronous pull stream and pushes into the async target.</summary>
    [PublicAPI]
    public static Fuse<IAsyncProducator<T2>> Next<T1, T2>(this Fuse<IAsyncConsumator<T1>> left, Pipe<IAsyncConsumator<T1>, IAsyncProducator<T2>> right)
    {
        return new Fuse<IAsyncProducator<T2>>(left.FlowSource, right.Invoke(left.Next));
    }

    #endregion

    #region Next: sync push input

    /// <summary>Chains a pipe owning a synchronous push producer.</summary>
    [PublicAPI]
    public static Fuse<IConsumator<T2>> Next<T1, T2>(this Fuse<IProducator<T1>> left, Pipe<IProducator<T1>, IConsumator<T2>> right)
    {
        return new Fuse<IConsumator<T2>>(left.FlowSource, right.Invoke(left.Next));
    }

    /// <summary>Chains a composite pipe: owns a synchronous push producer and emits a pull stream.</summary>
    [PublicAPI]
    public static Fuse<IAsyncConsumator<T2>> Next<T1, T2>(this Fuse<IProducator<T1>> left, Pipe<IProducator<T1>, IAsyncConsumator<T2>> right)
    {
        return new Fuse<IAsyncConsumator<T2>>(left.FlowSource, right.Invoke(left.Next));
    }

    /// <summary>Chains a pipe owning a synchronous push producer and pushing into the target.</summary>
    [PublicAPI]
    public static Fuse<IProducator<T2>> Next<T1, T2>(this Fuse<IProducator<T1>> left, Pipe<IProducator<T1>, IProducator<T2>> right)
    {
        return new Fuse<IProducator<T2>>(left.FlowSource, right.Invoke(left.Next));
    }

    /// <summary>Chains a pipe owning a synchronous push producer and pushing into the async target.</summary>
    [PublicAPI]
    public static Fuse<IAsyncProducator<T2>> Next<T1, T2>(this Fuse<IProducator<T1>> left, Pipe<IProducator<T1>, IAsyncProducator<T2>> right)
    {
        return new Fuse<IAsyncProducator<T2>>(left.FlowSource, right.Invoke(left.Next));
    }

    #endregion

    #region Next: async push input

    /// <summary>Chains a pipe owning an async push producer (a sync upstream feeds it via upcast).</summary>
    [PublicAPI]
    public static Fuse<IConsumator<T2>> Next<T1, T2>(this Fuse<IProducator<T1>> left, Pipe<IAsyncProducator<T1>, IConsumator<T2>> right)
    {
        return new Fuse<IConsumator<T2>>(left.FlowSource, right.Invoke(left.Next));
    }

    /// <summary>Chains a composite pipe owning an async push producer and emitting a pull stream (a sync upstream feeds it via upcast).</summary>
    [PublicAPI]
    public static Fuse<IAsyncConsumator<T2>> Next<T1, T2>(this Fuse<IProducator<T1>> left, Pipe<IAsyncProducator<T1>, IAsyncConsumator<T2>> right)
    {
        return new Fuse<IAsyncConsumator<T2>>(left.FlowSource, right.Invoke(left.Next));
    }

    /// <summary>Chains a pipe owning an async push producer and pushing into the target (a sync upstream feeds it via upcast).</summary>
    [PublicAPI]
    public static Fuse<IProducator<T2>> Next<T1, T2>(this Fuse<IProducator<T1>> left, Pipe<IAsyncProducator<T1>, IProducator<T2>> right)
    {
        return new Fuse<IProducator<T2>>(left.FlowSource, right.Invoke(left.Next));
    }

    /// <summary>Chains a pipe owning an async push producer and pushing into the async target (a sync upstream feeds it via upcast).</summary>
    [PublicAPI]
    public static Fuse<IAsyncProducator<T2>> Next<T1, T2>(this Fuse<IProducator<T1>> left, Pipe<IAsyncProducator<T1>, IAsyncProducator<T2>> right)
    {
        return new Fuse<IAsyncProducator<T2>>(left.FlowSource, right.Invoke(left.Next));
    }

    /// <summary>Chains a pipe owning an async push producer.</summary>
    [PublicAPI]
    public static Fuse<IConsumator<T2>> Next<T1, T2>(this Fuse<IAsyncProducator<T1>> left, Pipe<IAsyncProducator<T1>, IConsumator<T2>> right)
    {
        return new Fuse<IConsumator<T2>>(left.FlowSource, right.Invoke(left.Next));
    }

    /// <summary>Chains a composite pipe owning an async push producer and emitting a pull stream.</summary>
    [PublicAPI]
    public static Fuse<IAsyncConsumator<T2>> Next<T1, T2>(this Fuse<IAsyncProducator<T1>> left, Pipe<IAsyncProducator<T1>, IAsyncConsumator<T2>> right)
    {
        return new Fuse<IAsyncConsumator<T2>>(left.FlowSource, right.Invoke(left.Next));
    }

    /// <summary>Chains a pipe owning an async push producer and pushing into the target.</summary>
    [PublicAPI]
    public static Fuse<IProducator<T2>> Next<T1, T2>(this Fuse<IAsyncProducator<T1>> left, Pipe<IAsyncProducator<T1>, IProducator<T2>> right)
    {
        return new Fuse<IProducator<T2>>(left.FlowSource, right.Invoke(left.Next));
    }

    /// <summary>Chains a pipe owning an async push producer and pushing into the async target.</summary>
    [PublicAPI]
    public static Fuse<IAsyncProducator<T2>> Next<T1, T2>(this Fuse<IAsyncProducator<T1>> left, Pipe<IAsyncProducator<T1>, IAsyncProducator<T2>> right)
    {
        return new Fuse<IAsyncProducator<T2>>(left.FlowSource, right.Invoke(left.Next));
    }

    #endregion

    #region End

    /// <summary>Terminates the chain with a synchronous pull sink.</summary>
    [PublicAPI]
    public static void End<T>(this Fuse<IConsumator<T>> left, Sink<IConsumator<T>> right)
    {
        right.Invoke(left.FlowSource, left.Next);
    }

    /// <summary>Terminates the chain with an asynchronous pull sink.</summary>
    [PublicAPI]
    public static void End<T>(this Fuse<IAsyncConsumator<T>> left, Sink<IAsyncConsumator<T>> right)
    {
        right.Invoke(left.FlowSource, left.Next);
    }

    /// <summary>Terminates the chain with a synchronous push sink.</summary>
    [PublicAPI]
    public static void End<T>(this Fuse<IProducator<T>> left, Sink<IProducator<T>> right)
    {
        right.Invoke(left.FlowSource, left.Next);
    }

    /// <summary>Terminates the chain with an asynchronous push sink.</summary>
    [PublicAPI]
    public static void End<T>(this Fuse<IAsyncProducator<T>> left, Sink<IAsyncProducator<T>> right)
    {
        right.Invoke(left.FlowSource, left.Next);
    }

    #endregion

    #region Enumerator

    /// <summary>Opens a chain from an asynchronous enumerator source.</summary>
    [PublicAPI]
    public static Fuse<IAsyncEnumerator<T>> On<T>(this FlowSource source, Source<IAsyncEnumerator<T>> right)
    {
        return new Fuse<IAsyncEnumerator<T>>(source, right.Invoke());
    }

    /// <summary>Chains a pipe over asynchronous enumerators.</summary>
    [PublicAPI]
    public static Fuse<IAsyncEnumerator<T2>> Next<T1, T2>(this Fuse<IAsyncEnumerator<T1>> left, Pipe<IAsyncEnumerator<T1>, IAsyncEnumerator<T2>> right)
    {
        return new Fuse<IAsyncEnumerator<T2>>(left.FlowSource, right.Invoke(left.Next));
    }

    /// <summary>Bridges an enumerator upstream into an async-consumator-input pipe (wrapped by the adapter).</summary>
    [PublicAPI]
    public static Fuse<IConsumator<T2>> Next<T1, T2>(this Fuse<IAsyncEnumerator<T1>> left, Pipe<IAsyncConsumator<T1>, IConsumator<T2>> right)
    {
        return new Fuse<IConsumator<T2>>(left.FlowSource, right.Invoke(left.Next));
    }

    /// <summary>Bridges an enumerator upstream into an async-consumator-input pipe (wrapped by the adapter).</summary>
    [PublicAPI]
    public static Fuse<IAsyncConsumator<T2>> Next<T1, T2>(this Fuse<IAsyncEnumerator<T1>> left, Pipe<IAsyncConsumator<T1>, IAsyncConsumator<T2>> right)
    {
        return new Fuse<IAsyncConsumator<T2>>(left.FlowSource, right.Invoke(left.Next));
    }

/// <summary>Bridges an enumerator upstream into an async-consumator-input pipe (wrapped by the adapter).</summary>
    [PublicAPI]
    public static Fuse<IAsyncProducator<T2>> Next<T1, T2>(this Fuse<IAsyncEnumerator<T1>> left, Pipe<IAsyncConsumator<T1>, IAsyncProducator<T2>> right)
    {
        return new Fuse<IAsyncProducator<T2>>(left.FlowSource, right.Invoke(left.Next));
    }

    /// <summary>Bridges an async-consumator upstream into an enumerator-input pipe (wrapped by the adapter).</summary>
    [PublicAPI]
    public static Fuse<IAsyncEnumerator<T2>> Next<T1, T2>(this Fuse<IAsyncConsumator<T1>> left, Pipe<IAsyncEnumerator<T1>, IAsyncEnumerator<T2>> right)
    {
        return new Fuse<IAsyncEnumerator<T2>>(left.FlowSource, right.Invoke(left.Next));
    }

    /// <summary>Bridges an enumerator upstream into a hybrid async-consumator-input pipe (wrapped by the adapter).</summary>
    [PublicAPI]
    public static Fuse<IProducator<T2>> Next<T1, T2>(this Fuse<IAsyncEnumerator<T1>> left, Pipe<IAsyncConsumator<T1>, IProducator<T2>> right)
    {
        return new Fuse<IProducator<T2>>(left.FlowSource, right.Invoke(left.Next));
    }

    /// <summary>Terminates the chain with an asynchronous enumerator sink.</summary>
    [PublicAPI]
    public static void End<T>(this Fuse<IAsyncEnumerator<T>> left, Sink<IAsyncEnumerator<T>> right)
    {
        right.Invoke(left.FlowSource, left.Next);
    }

    #endregion
}