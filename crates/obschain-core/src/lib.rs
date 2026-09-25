pub mod amount;
pub mod error;
pub mod event;
pub mod fee;
pub mod incident;
pub mod observation;
pub mod source;
pub mod watch;

pub use amount::{
    btc_str_to_sats, calculate_satoshi_days, safe_btc_f64_to_sats, satoshi_days_to_btc_days,
    AmountError, MAX_MONEY_SATS, SATS_PER_BTC,
};
pub use error::CoreError;
pub use event::{
    ChainEvent, ConfidenceLevel, ConsolidationMetadata, DormantClassification,
    DormantCoinsMetadata, EventSeverity, EventType, ExtremeFeeMetadata, ExtremeFeeTriggerType,
    FanOutMetadata, ReplacementMetadata,
};
pub use fee::{calculate_fee_rate_sat_vb, calculate_vsize_from_weight};
pub use incident::{
    Chain, Evidence, EvidenceType, FundMovement, GraphEdge, GraphEdgeType, GraphNode,
    GraphNodeType, Incident, IncidentBlock, IncidentEntity, IncidentGraph, IncidentStatus,
    IncidentTransaction, IncidentUpdate, OnChainMessage, ProvenanceClassification, RecoverySummary,
    Source, SourceCategory, StructuredClaimsSummary, TechnicalFinding, TimelineCategory,
    TimelineEntry, TimelineEvent, TransactionRole,
};
pub use observation::{
    BlockObservation, MempoolObservation, Observation, ReorgObservation, SpentOutputContext,
    TransactionObservation, TransactionReplacement, TxInputObservation, TxOutputObservation,
};
pub use source::{ObservationSource, ObservationWitness, SourceHealthState};
pub use watch::{
    ActivityStatus, CorrelationStrength, IncidentActivity, IncidentActivityType, IncidentAlert,
    PublicWatchTarget, WatchTarget, WatchTargetKind,
};
