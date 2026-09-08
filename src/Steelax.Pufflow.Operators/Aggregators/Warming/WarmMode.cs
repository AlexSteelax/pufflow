namespace Steelax.Pufflow.Operators.Aggregators.Warming;

/// <summary>
///     Controls how the warmer's <c>WarmNext</c> handles the current open tail segment during a processing
///     pass.
/// </summary>
internal enum WarmMode
{
    /// <summary>
    ///     Leave the open tail accepting: a partially filled segment is handed to a job only when it reaches
    ///     full capacity (or an earlier explicit seal). Used for steady-state processing where segments are
    ///     filled by the source until they are full.
    /// </summary>
    Normal,

    /// <summary>
    ///     Seal the current open tail so it can be handed to a job even if only partially filled (subject to a
    ///     free job slot), then continue with a fresh segment for subsequent keys. Used when the source stalls
    ///     (segment linger elapsed) or is exhausted, to avoid waiting for full capacity.
    /// </summary>
    SealTail
}
