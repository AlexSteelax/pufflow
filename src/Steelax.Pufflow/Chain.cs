using System.Collections.Generic;
using System.ComponentModel;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow;

/// <summary>
///     Defines the <em>pull</em> link of a composed chain. When invoked, the upstream produces a
///     synchronous pull reader (<see cref="IConsumator{T}" />) that the downstream stage can read from.
///     The instance representing the upstream block is constructed and registered at invocation time.
/// </summary>
/// <typeparam name="T">The element type flowing through the pull reader.</typeparam>
/// <param name="flow">The flow source owning cancellation and resource registration for the chain.</param>
/// <param name="reader">The pull reader handed out by the upstream stage.</param>
[PublicAPI]
[EditorBrowsable(EditorBrowsableState.Never)]
public delegate void ConsumatorChain<T>(FlowSource flow, out IConsumator<T> reader);

/// <summary>
///     Defines the asynchronous <em>pull</em> link of a composed chain. When invoked, the upstream
///     produces an asynchronous pull reader (<see cref="IAsyncConsumator{T}" />) for the downstream stage.
/// </summary>
/// <typeparam name="T">The element type flowing through the pull reader.</typeparam>
/// <param name="flow">The flow source owning cancellation and resource registration for the chain.</param>
/// <param name="reader">The asynchronous pull reader handed out by the upstream stage.</param>
[PublicAPI]
[EditorBrowsable(EditorBrowsableState.Never)]
public delegate void AsyncConsumatorChain<T>(FlowSource flow, out IAsyncConsumator<T> reader);

/// <summary>
///     Defines the <em>push</em> link of a composed chain. When invoked, the caller supplies the push
///     target (<see cref="IProducator{T}" />) and the upstream stage writes its data into it.
/// </summary>
/// <typeparam name="T">The element type flowing through the push target.</typeparam>
/// <param name="flow">The flow source owning cancellation and resource registration for the chain.</param>
/// <param name="writer">The push target the upstream stage writes into.</param>
[PublicAPI]
[EditorBrowsable(EditorBrowsableState.Never)]
public delegate void ProducatorChain<out T>(FlowSource flow, IProducator<T> writer);

/// <summary>
///     Defines the asynchronous <em>push</em> link of a composed chain. When invoked, the caller supplies
///     the asynchronous push target (<see cref="IAsyncProducator{T}" />) and the upstream stage writes its
///     data into it.
/// </summary>
/// <typeparam name="T">The element type flowing through the push target.</typeparam>
/// <param name="flow">The flow source owning cancellation and resource registration for the chain.</param>
/// <param name="writer">The asynchronous push target the upstream stage writes into.</param>
[PublicAPI]
[EditorBrowsable(EditorBrowsableState.Never)]
public delegate void AsyncProducatorChain<out T>(FlowSource flow, IAsyncProducator<T> writer);

/// <summary>
///     Defines the <em>enumerator</em> link of a composed chain. When invoked, the upstream produces a
///     standard .NET asynchronous pull enumerator (<see cref="IAsyncEnumerator{T}" />) that the downstream
///     stage consumes. This is the bridge between Pufflow's consumator model and the standard .NET
///     async-enumerator world.
/// </summary>
/// <typeparam name="T">The element type flowing through the enumerator.</typeparam>
/// <param name="flow">The flow source owning cancellation and resource registration for the chain.</param>
/// <param name="enumerator">The asynchronous enumerator handed out by the upstream stage.</param>
[PublicAPI]
[EditorBrowsable(EditorBrowsableState.Never)]
public delegate void EnumeratorChain<T>(FlowSource flow, out IAsyncEnumerator<T> enumerator);