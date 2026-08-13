namespace Steelax.Pufflow;

/// <summary>
///     Marker base for flow metadata. A concrete <see cref="FlowMetaNode" /> describes a single pipeline
///     node; a <see cref="FlowMetaCollection" /> groups nodes for reverse-order (push) chain building.
///     The flow context is not part of the metadata — it is passed separately at invocation time.
/// </summary>
internal abstract class FlowMeta;
