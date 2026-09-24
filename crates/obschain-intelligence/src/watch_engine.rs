use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use chrono::Utc;
use obschain_core::{
    ActivityStatus, BlockObservation, CorrelationStrength, EventSeverity, Incident,
    IncidentActivity, IncidentActivityType, IncidentAlert, IncidentUpdate, ObservationSource,
    ProvenanceClassification, TimelineCategory, TimelineEntry, TransactionObservation,
    TransactionReplacement, WatchTarget, WatchTargetKind,
};
use tracing::debug;
use uuid::Uuid;

/// Tracked descendant outpoint with bounded distance from origin incident watch target.
#[derive(Debug, Clone)]
pub struct TrackedDescendantOutpoint {
    pub incident_id: Uuid,
    pub case_id: String,
    pub origin_target_id: Uuid,
    pub depth: usize,
    pub confidence: ProvenanceClassification,
    pub created_at: chrono::DateTime<Utc>,
}

/// Core engine for observing live Bitcoin activity and correlating it deterministically
/// against incident watch targets.
pub struct IncidentWatchEngine {
    /// Exact OutPoint matching: (txid_hex_lowercase, vout) -> targets
    outpoint_index: HashMap<(String, u32), Vec<Arc<WatchTarget>>>,
    /// Exact ScriptPubKey matching: script_hex_lowercase -> targets
    script_index: HashMap<String, Vec<Arc<WatchTarget>>>,
    /// Address matching: address -> targets
    address_index: HashMap<String, Vec<Arc<WatchTarget>>>,
    /// Specific TxID matching: txid_hex_lowercase -> targets
    txid_index: HashMap<String, Vec<Arc<WatchTarget>>>,
    /// All targets keyed by target ID
    all_targets: HashMap<Uuid, Arc<WatchTarget>>,
    /// Tracked transaction descendants: (txid_hex_lowercase, vout) -> metadata
    descendant_outpoints: HashMap<(String, u32), TrackedDescendantOutpoint>,
    /// FIFO order queue to enforce bounded memory on descendant tracking
    descendant_fifo: VecDeque<(String, u32)>,
    /// Maximum descendant DAG depth to trace (env: OBSCHAIN_INCIDENT_FOLLOW_DEPTH, default 3, max 5)
    max_descendant_depth: usize,
    /// Maximum number of active tracked descendant outpoints
    max_tracked_descendants: usize,
    /// Incident titles cache for generating human-readable alerts: case_id -> title
    incident_titles: HashMap<String, String>,
}

impl Default for IncidentWatchEngine {
    fn default() -> Self {
        Self::from_env()
    }
}

impl IncidentWatchEngine {
    pub const DEFAULT_FOLLOW_DEPTH: usize = 3;
    pub const MAX_ALLOWED_FOLLOW_DEPTH: usize = 5;
    pub const DEFAULT_MAX_TRACKED_DESCENDANTS: usize = 1_000;

    /// Initializes engine using environment variable `OBSCHAIN_INCIDENT_FOLLOW_DEPTH` (default 3, max 5).
    pub fn from_env() -> Self {
        let depth = std::env::var("OBSCHAIN_INCIDENT_FOLLOW_DEPTH")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(Self::DEFAULT_FOLLOW_DEPTH)
            .clamp(1, Self::MAX_ALLOWED_FOLLOW_DEPTH);

        Self::with_follow_depth(depth)
    }

    /// Initializes engine with explicit descendant follow depth limit.
    pub fn with_follow_depth(max_depth: usize) -> Self {
        let clamped_depth = max_depth.clamp(1, Self::MAX_ALLOWED_FOLLOW_DEPTH);
        Self {
            outpoint_index: HashMap::new(),
            script_index: HashMap::new(),
            address_index: HashMap::new(),
            txid_index: HashMap::new(),
            all_targets: HashMap::new(),
            descendant_outpoints: HashMap::new(),
            descendant_fifo: VecDeque::with_capacity(Self::DEFAULT_MAX_TRACKED_DESCENDANTS),
            max_descendant_depth: clamped_depth,
            max_tracked_descendants: Self::DEFAULT_MAX_TRACKED_DESCENDANTS,
            incident_titles: HashMap::new(),
        }
    }

    /// Registers a case title for human-readable alert enrichment.
    pub fn register_incident_title(
        &mut self,
        case_id: impl Into<String>,
        title: impl Into<String>,
    ) {
        self.incident_titles.insert(case_id.into(), title.into());
    }

    /// Adds a single watch target to internal indices.
    pub fn add_target(&mut self, target: WatchTarget) {
        if !target.active {
            return;
        }

        let target_arc = Arc::new(target);
        self.all_targets.insert(target_arc.id, target_arc.clone());

        match &target_arc.kind {
            WatchTargetKind::OutPoint { txid, vout } => {
                let key = (txid.to_lowercase(), *vout);
                self.outpoint_index
                    .entry(key)
                    .or_default()
                    .push(target_arc.clone());
            }
            WatchTargetKind::ScriptPubKey { script_hex } => {
                let key = script_hex.to_lowercase();
                self.script_index
                    .entry(key)
                    .or_default()
                    .push(target_arc.clone());
            }
            WatchTargetKind::Address { address, .. } => {
                let key = address.clone();
                self.address_index
                    .entry(key)
                    .or_default()
                    .push(target_arc.clone());
            }
            WatchTargetKind::Transaction { txid } => {
                let key = txid.to_lowercase();
                self.txid_index
                    .entry(key)
                    .or_default()
                    .push(target_arc.clone());
            }
        }
    }

    /// Loads multiple watch targets.
    pub fn load_targets(&mut self, targets: impl IntoIterator<Item = WatchTarget>) {
        for target in targets {
            self.add_target(target);
        }
    }

    /// Removes a watch target by ID.
    pub fn remove_target(&mut self, target_id: &Uuid) {
        if let Some(target) = self.all_targets.remove(target_id) {
            match &target.kind {
                WatchTargetKind::OutPoint { txid, vout } => {
                    let key = (txid.to_lowercase(), *vout);
                    if let Some(vec) = self.outpoint_index.get_mut(&key) {
                        vec.retain(|t| t.id != *target_id);
                        if vec.is_empty() {
                            self.outpoint_index.remove(&key);
                        }
                    }
                }
                WatchTargetKind::ScriptPubKey { script_hex } => {
                    let key = script_hex.to_lowercase();
                    if let Some(vec) = self.script_index.get_mut(&key) {
                        vec.retain(|t| t.id != *target_id);
                        if vec.is_empty() {
                            self.script_index.remove(&key);
                        }
                    }
                }
                WatchTargetKind::Address { address, .. } => {
                    let key = address.clone();
                    if let Some(vec) = self.address_index.get_mut(&key) {
                        vec.retain(|t| t.id != *target_id);
                        if vec.is_empty() {
                            self.address_index.remove(&key);
                        }
                    }
                }
                WatchTargetKind::Transaction { txid } => {
                    let key = txid.to_lowercase();
                    if let Some(vec) = self.txid_index.get_mut(&key) {
                        vec.retain(|t| t.id != *target_id);
                        if vec.is_empty() {
                            self.txid_index.remove(&key);
                        }
                    }
                }
            }
        }
    }

    /// Count of currently monitored targets.
    pub fn active_target_count(&self) -> usize {
        self.all_targets.len()
    }

    /// Count of actively tracked descendant outpoints.
    pub fn tracked_descendants_count(&self) -> usize {
        self.descendant_outpoints.len()
    }

    /// Maximum follow depth configured.
    pub fn max_descendant_depth(&self) -> usize {
        self.max_descendant_depth
    }

    /// Enrolls outputs of a transaction into the descendant tracking registry.
    fn enroll_descendants(
        &mut self,
        tx: &TransactionObservation,
        incident_id: Uuid,
        case_id: &str,
        origin_target_id: Uuid,
        depth: usize,
        confidence: ProvenanceClassification,
    ) {
        if depth > self.max_descendant_depth {
            return;
        }

        let now = Utc::now();
        for (vout, out) in tx.outputs.iter().enumerate() {
            // Do not track unspendable OP_RETURN outputs
            if out.is_op_return() {
                continue;
            }

            let key = (tx.txid.to_lowercase(), vout as u32);
            if self.descendant_fifo.len() >= self.max_tracked_descendants {
                if let Some(old_key) = self.descendant_fifo.pop_front() {
                    self.descendant_outpoints.remove(&old_key);
                }
            }

            self.descendant_fifo.push_back(key.clone());
            self.descendant_outpoints.insert(
                key,
                TrackedDescendantOutpoint {
                    incident_id,
                    case_id: case_id.to_string(),
                    origin_target_id,
                    depth,
                    confidence,
                    created_at: now,
                },
            );
        }
    }

    /// Evaluates an observed transaction against all indices.
    /// Returns any matched activities and generated alerts.
    pub fn process_transaction(
        &mut self,
        tx: &TransactionObservation,
        block_height: Option<u64>,
        block_hash: Option<&str>,
    ) -> Vec<(IncidentActivity, Option<IncidentAlert>)> {
        let mut results = Vec::new();
        let now = Utc::now();
        let status = if block_height.is_some() {
            ActivityStatus::Confirmed
        } else {
            ActivityStatus::Mempool
        };
        let fallback_source = ObservationSource::new("bitcoin", "unknown", None);
        let obs_source = tx.source.as_ref().unwrap_or(&fallback_source).clone();

        // 1. Direct TxID Match
        let txid_key = tx.txid.to_lowercase();
        if let Some(targets) = self.txid_index.get(&txid_key).cloned() {
            for target in targets {
                let activity_type = if block_height.is_some() {
                    IncidentActivityType::WatchedTransactionConfirmed
                } else {
                    IncidentActivityType::WatchedTransactionObserved
                };

                let description = format!(
                    "Monitored transaction {} observed on-chain ({:?})",
                    &tx.txid, status
                );

                let dedup_key = IncidentActivity::generate_dedup_key(
                    &target.incident_id,
                    &activity_type,
                    Some(&tx.txid),
                    &target.id,
                );

                let activity = IncidentActivity {
                    id: Uuid::new_v4(),
                    incident_id: target.incident_id,
                    case_id: target.case_id.clone(),
                    activity_type,
                    observed_at: now,
                    trigger_txid: Some(tx.txid.clone()),
                    block_height,
                    block_hash: block_hash.map(|s| s.to_string()),
                    value_sats: Some(tx.total_output_sats),
                    watch_target_id: target.id,
                    confidence: target.classification,
                    correlation_strength: CorrelationStrength::Direct,
                    status,
                    source: obs_source.clone(),
                    evidence: target.evidence_id.map(|e| vec![e]).unwrap_or_default(),
                    description: description.clone(),
                    details: Some(serde_json::json!({
                        "txid": tx.txid,
                        "total_output_sats": tx.total_output_sats,
                        "fee_sats": tx.fee_sats,
                        "vin_count": tx.inputs.len(),
                        "vout_count": tx.outputs.len(),
                    })),
                    dedup_key,
                };

                let alert = self.create_alert_if_significant(&activity, &target);
                results.push((activity, alert));
            }
        }

        // 2. Evaluate Inputs: Watched Outpoint Spends & Address Spends
        for (vin, input) in tx.inputs.iter().enumerate() {
            let outpoint_key = (input.txid.to_lowercase(), input.vout);

            // A) Exact OutPoint Match
            if let Some(targets) = self.outpoint_index.get(&outpoint_key).cloned() {
                for target in targets {
                    let activity_type = IncidentActivityType::WatchedOutpointSpent;
                    let description = format!(
                        "Watched outpoint {}:{} spent by transaction {} (input #{})",
                        &input.txid, input.vout, &tx.txid, vin
                    );

                    let dedup_key = IncidentActivity::generate_dedup_key(
                        &target.incident_id,
                        &activity_type,
                        Some(&tx.txid),
                        &target.id,
                    );

                    let activity = IncidentActivity {
                        id: Uuid::new_v4(),
                        incident_id: target.incident_id,
                        case_id: target.case_id.clone(),
                        activity_type,
                        observed_at: now,
                        trigger_txid: Some(tx.txid.clone()),
                        block_height,
                        block_hash: block_hash.map(|s| s.to_string()),
                        value_sats: input.prev_out_value_sats,
                        watch_target_id: target.id,
                        confidence: target.classification,
                        correlation_strength: CorrelationStrength::Direct,
                        status,
                        source: obs_source.clone(),
                        evidence: target.evidence_id.map(|e| vec![e]).unwrap_or_default(),
                        description,
                        details: Some(serde_json::json!({
                            "spent_outpoint": {
                                "txid": input.txid,
                                "vout": input.vout,
                            },
                            "vin_index": vin,
                            "spending_txid": tx.txid,
                            "value_sats": input.prev_out_value_sats,
                        })),
                        dedup_key,
                    };

                    let alert = self.create_alert_if_significant(&activity, &target);

                    // Enroll first layer of descendants
                    self.enroll_descendants(
                        tx,
                        target.incident_id,
                        &target.case_id,
                        target.id,
                        1,
                        target.classification,
                    );

                    results.push((activity, alert));
                }
            }

            // B) Bounded Descendant Outpoint Spend
            if let Some(descendant) = self.descendant_outpoints.remove(&outpoint_key) {
                let activity_type = IncidentActivityType::NewDescendantObserved;
                let description = format!(
                    "Incident descendant outpoint {}:{} (depth {}) spent by transaction {}",
                    &input.txid, input.vout, descendant.depth, &tx.txid
                );

                let dedup_key = IncidentActivity::generate_dedup_key(
                    &descendant.incident_id,
                    &activity_type,
                    Some(&tx.txid),
                    &descendant.origin_target_id,
                );

                let activity = IncidentActivity {
                    id: Uuid::new_v4(),
                    incident_id: descendant.incident_id,
                    case_id: descendant.case_id.clone(),
                    activity_type,
                    observed_at: now,
                    trigger_txid: Some(tx.txid.clone()),
                    block_height,
                    block_hash: block_hash.map(|s| s.to_string()),
                    value_sats: input.prev_out_value_sats,
                    watch_target_id: descendant.origin_target_id,
                    confidence: descendant.confidence,
                    correlation_strength: CorrelationStrength::Structural,
                    status,
                    source: obs_source.clone(),
                    evidence: vec![],
                    description,
                    details: Some(serde_json::json!({
                        "descendant_depth": descendant.depth,
                        "spent_outpoint": {
                            "txid": input.txid,
                            "vout": input.vout,
                        },
                        "spending_txid": tx.txid,
                    })),
                    dedup_key,
                };

                let target_opt = self.all_targets.get(&descendant.origin_target_id);
                let alert = if let Some(target) = target_opt {
                    self.create_alert_if_significant(&activity, target)
                } else {
                    None
                };

                // Enroll next generation of descendants
                if descendant.depth < self.max_descendant_depth {
                    self.enroll_descendants(
                        tx,
                        descendant.incident_id,
                        &descendant.case_id,
                        descendant.origin_target_id,
                        descendant.depth + 1,
                        descendant.confidence,
                    );
                }

                results.push((activity, alert));
            }

            // C) Watched Address Spent
            if let Some(ref addr) = input.prev_out_address {
                if let Some(targets) = self.address_index.get(addr).cloned() {
                    for target in targets {
                        let activity_type = IncidentActivityType::WatchedAddressSpent;
                        let description = format!(
                            "Monitored address {} spent {} sats in tx {}",
                            addr,
                            input.prev_out_value_sats.unwrap_or(0),
                            &tx.txid
                        );

                        let dedup_key = IncidentActivity::generate_dedup_key(
                            &target.incident_id,
                            &activity_type,
                            Some(&tx.txid),
                            &target.id,
                        );

                        // Epistemological rule: Heuristic address matches NEVER become Direct proof of ownership
                        let correlation_strength = match target.classification {
                            ProvenanceClassification::OnChainVerified => {
                                CorrelationStrength::Direct
                            }
                            ProvenanceClassification::OfficiallyAttributed => {
                                CorrelationStrength::Structural
                            }
                            ProvenanceClassification::ReputableReporting
                            | ProvenanceClassification::Heuristic
                            | ProvenanceClassification::Unverified
                            | ProvenanceClassification::Disputed => CorrelationStrength::Heuristic,
                        };

                        let activity = IncidentActivity {
                            id: Uuid::new_v4(),
                            incident_id: target.incident_id,
                            case_id: target.case_id.clone(),
                            activity_type,
                            observed_at: now,
                            trigger_txid: Some(tx.txid.clone()),
                            block_height,
                            block_hash: block_hash.map(|s| s.to_string()),
                            value_sats: input.prev_out_value_sats,
                            watch_target_id: target.id,
                            confidence: target.classification,
                            correlation_strength,
                            status,
                            source: obs_source.clone(),
                            evidence: target.evidence_id.map(|e| vec![e]).unwrap_or_default(),
                            description,
                            details: Some(serde_json::json!({
                                "address": addr,
                                "vin_index": vin,
                                "spent_txid": tx.txid,
                                "co_spent_inputs_count": tx.inputs.len(),
                            })),
                            dedup_key,
                        };

                        let alert = self.create_alert_if_significant(&activity, &target);
                        results.push((activity, alert));
                    }
                }
            }
        }

        // 3. Evaluate Outputs: Watched ScriptPubKeys & Address Receives
        for (vout, output) in tx.outputs.iter().enumerate() {
            // A) Exact ScriptPubKey Match
            if let Some(ref script_hex) = output.scriptpubkey_hex {
                let script_key = script_hex.to_lowercase();
                if let Some(targets) = self.script_index.get(&script_key).cloned() {
                    for target in targets {
                        let activity_type = IncidentActivityType::WatchedScriptReceived;
                        let description = format!(
                            "Watched scriptPubKey received {} sats in tx {} (output #{})",
                            output.value_sats, &tx.txid, vout
                        );

                        let dedup_key = IncidentActivity::generate_dedup_key(
                            &target.incident_id,
                            &activity_type,
                            Some(&tx.txid),
                            &target.id,
                        );

                        let activity = IncidentActivity {
                            id: Uuid::new_v4(),
                            incident_id: target.incident_id,
                            case_id: target.case_id.clone(),
                            activity_type,
                            observed_at: now,
                            trigger_txid: Some(tx.txid.clone()),
                            block_height,
                            block_hash: block_hash.map(|s| s.to_string()),
                            value_sats: Some(output.value_sats),
                            watch_target_id: target.id,
                            confidence: target.classification,
                            correlation_strength: CorrelationStrength::Direct,
                            status,
                            source: obs_source.clone(),
                            evidence: target.evidence_id.map(|e| vec![e]).unwrap_or_default(),
                            description,
                            details: Some(serde_json::json!({
                                "script_pubkey": script_hex,
                                "vout_index": vout,
                                "value_sats": output.value_sats,
                            })),
                            dedup_key,
                        };

                        let alert = self.create_alert_if_significant(&activity, &target);
                        results.push((activity, alert));
                    }
                }
            }

            // B) Address Received Match
            if let Some(ref addr) = output.address {
                if let Some(targets) = self.address_index.get(addr).cloned() {
                    for target in targets {
                        let activity_type = IncidentActivityType::WatchedAddressReceived;
                        let description = format!(
                            "Monitored address {} received {} sats in tx {} (output #{})",
                            addr, output.value_sats, &tx.txid, vout
                        );

                        let dedup_key = IncidentActivity::generate_dedup_key(
                            &target.incident_id,
                            &activity_type,
                            Some(&tx.txid),
                            &target.id,
                        );

                        let correlation_strength = match target.classification {
                            ProvenanceClassification::OnChainVerified => {
                                CorrelationStrength::Direct
                            }
                            ProvenanceClassification::OfficiallyAttributed => {
                                CorrelationStrength::Structural
                            }
                            ProvenanceClassification::ReputableReporting
                            | ProvenanceClassification::Heuristic
                            | ProvenanceClassification::Unverified
                            | ProvenanceClassification::Disputed => CorrelationStrength::Heuristic,
                        };

                        let activity = IncidentActivity {
                            id: Uuid::new_v4(),
                            incident_id: target.incident_id,
                            case_id: target.case_id.clone(),
                            activity_type,
                            observed_at: now,
                            trigger_txid: Some(tx.txid.clone()),
                            block_height,
                            block_hash: block_hash.map(|s| s.to_string()),
                            value_sats: Some(output.value_sats),
                            watch_target_id: target.id,
                            confidence: target.classification,
                            correlation_strength,
                            status,
                            source: obs_source.clone(),
                            evidence: target.evidence_id.map(|e| vec![e]).unwrap_or_default(),
                            description,
                            details: Some(serde_json::json!({
                                "address": addr,
                                "vout_index": vout,
                                "value_sats": output.value_sats,
                            })),
                            dedup_key,
                        };

                        let alert = self.create_alert_if_significant(&activity, &target);
                        results.push((activity, alert));
                    }
                }
            }
        }

        results
    }

    /// Evaluates transaction replacements (RBF) against watch targets.
    pub fn process_replacement(
        &mut self,
        repl: &TransactionReplacement,
    ) -> Vec<(IncidentActivity, Option<IncidentAlert>)> {
        let mut results = Vec::new();
        let now = Utc::now();
        let fallback_source = ObservationSource::new("bitcoin", "unknown", None);
        let obs_source = repl.source.as_ref().unwrap_or(&fallback_source).clone();

        // Check if replacement txid or any replaced txid is watched
        let mut matched_target: Option<Arc<WatchTarget>> = None;
        let repl_key = repl.replacement_txid.to_lowercase();
        if let Some(targets) = self.txid_index.get(&repl_key) {
            matched_target = targets.first().cloned();
        }

        if matched_target.is_none() {
            for old_txid in &repl.replaced_txids {
                let old_key = old_txid.to_lowercase();
                if let Some(targets) = self.txid_index.get(&old_key) {
                    matched_target = targets.first().cloned();
                    break;
                }
            }
        }

        if let Some(target) = matched_target {
            let activity_type = IncidentActivityType::TransactionReplacement;
            let description = format!(
                "RBF replacement observed for incident tx {}: replaced by {} (fee delta: {} sats)",
                &repl.replaced_txids.join(", "),
                &repl.replacement_txid,
                repl.fee_delta_sats
            );

            let dedup_key = IncidentActivity::generate_dedup_key(
                &target.incident_id,
                &activity_type,
                Some(&repl.replacement_txid),
                &target.id,
            );

            let activity = IncidentActivity {
                id: Uuid::new_v4(),
                incident_id: target.incident_id,
                case_id: target.case_id.clone(),
                activity_type,
                observed_at: now,
                trigger_txid: Some(repl.replacement_txid.clone()),
                block_height: None,
                block_hash: None,
                value_sats: None,
                watch_target_id: target.id,
                confidence: target.classification,
                correlation_strength: CorrelationStrength::Structural,
                status: ActivityStatus::Mempool,
                source: obs_source,
                evidence: target.evidence_id.map(|e| vec![e]).unwrap_or_default(),
                description,
                details: Some(serde_json::json!({
                    "replacement_txid": repl.replacement_txid,
                    "replaced_txids": repl.replaced_txids,
                    "fee_delta_sats": repl.fee_delta_sats,
                })),
                dedup_key,
            };

            let alert = self.create_alert_if_significant(&activity, &target);
            results.push((activity, alert));
        }

        results
    }

    /// Evaluates a block observation against watch targets.
    pub fn process_block(
        &mut self,
        block: &BlockObservation,
    ) -> Vec<(IncidentActivity, Option<IncidentAlert>)> {
        // Block processing can confirm pending mempool observations
        debug!(
            height = block.height,
            "Processing block in IncidentWatchEngine"
        );
        Vec::new()
    }

    /// Computes alert severity deterministically based on activity type, correlation strength,
    /// confidence rating, and monetary value moved.
    pub fn calculate_alert_severity(
        activity_type: IncidentActivityType,
        correlation: CorrelationStrength,
        _confidence: ProvenanceClassification,
        value_sats: Option<u64>,
    ) -> EventSeverity {
        let value = value_sats.unwrap_or(0);

        // 1. Direct OutPoint Spend of significant funds
        if activity_type == IncidentActivityType::WatchedOutpointSpent {
            if value >= 10_000_000_000 {
                // >= 100 BTC
                return EventSeverity::Critical;
            }
            if value >= 1_000_000_000 {
                // >= 10 BTC
                return EventSeverity::High;
            }
            return EventSeverity::High;
        }

        // 2. Heuristic correlations capped at Medium severity to avoid false sense of certainty
        if correlation == CorrelationStrength::Heuristic {
            return EventSeverity::Medium;
        }

        // 3. High-value movements on watched scripts or addresses
        if value >= 10_000_000_000 && correlation == CorrelationStrength::Direct {
            return EventSeverity::Critical;
        }
        if value >= 1_000_000_000 {
            return EventSeverity::High;
        }

        // 4. Default based on activity type
        match activity_type {
            IncidentActivityType::WatchedOutpointSpent => EventSeverity::High,
            IncidentActivityType::WatchedScriptReceived => EventSeverity::Medium,
            IncidentActivityType::WatchedAddressReceived => EventSeverity::Medium,
            IncidentActivityType::WatchedAddressSpent => EventSeverity::Medium,
            IncidentActivityType::WatchedTransactionConfirmed => EventSeverity::Medium,
            IncidentActivityType::WatchedTransactionObserved => EventSeverity::Low,
            IncidentActivityType::TransactionReplacement => EventSeverity::Medium,
            IncidentActivityType::NewDescendantObserved => {
                if value >= 500_000_000 {
                    EventSeverity::High
                } else {
                    EventSeverity::Low
                }
            }
        }
    }

    /// Creates an IncidentAlert entity if the activity is deemed significant.
    fn create_alert_if_significant(
        &self,
        activity: &IncidentActivity,
        target: &WatchTarget,
    ) -> Option<IncidentAlert> {
        let severity = Self::calculate_alert_severity(
            activity.activity_type,
            activity.correlation_strength,
            activity.confidence,
            activity.value_sats,
        );

        let default_title = format!("Incident {}", activity.case_id);
        let incident_title = self
            .incident_titles
            .get(&activity.case_id)
            .unwrap_or(&default_title)
            .clone();

        let alert_title = format!(
            "[{}] {}",
            activity.case_id,
            match activity.activity_type {
                IncidentActivityType::WatchedOutpointSpent => "Watched Outpoint Spent",
                IncidentActivityType::WatchedScriptReceived => "Watched Script Received Funds",
                IncidentActivityType::WatchedAddressReceived => "Monitored Address Received Funds",
                IncidentActivityType::WatchedAddressSpent => "Monitored Address Spent Funds",
                IncidentActivityType::WatchedTransactionConfirmed =>
                    "Watched Transaction Confirmed",
                IncidentActivityType::WatchedTransactionObserved => "Watched Transaction Observed",
                IncidentActivityType::TransactionReplacement => "Transaction Replacement (RBF)",
                IncidentActivityType::NewDescendantObserved => "New Descendant Activity",
            }
        );

        let summary = format!(
            "{}. Target: {} (Provenance: {:?}, Strength: {:?})",
            activity.description,
            target.label.as_deref().unwrap_or(&target.kind.summary()),
            activity.confidence,
            activity.correlation_strength
        );

        Some(IncidentAlert {
            id: Uuid::new_v4(),
            incident_id: activity.incident_id,
            case_id: activity.case_id.clone(),
            incident_title,
            activity_id: activity.id,
            severity,
            title: alert_title,
            summary,
            confidence: activity.confidence,
            correlation_strength: activity.correlation_strength,
            observed_at: activity.observed_at,
            value_sats: activity.value_sats,
            trigger_txid: activity.trigger_txid.clone(),
        })
    }

    /// Transforms an activity record into an append-only timeline entry for incident intelligence.
    pub fn generate_timeline_entry(activity: &IncidentActivity) -> TimelineEntry {
        let title = match activity.activity_type {
            IncidentActivityType::WatchedOutpointSpent => "Watched Incident UTXO Spent",
            IncidentActivityType::WatchedScriptReceived => "Incident Script Received Funds",
            IncidentActivityType::WatchedAddressReceived => {
                "Monitored Incident Address Received Funds"
            }
            IncidentActivityType::WatchedAddressSpent => "Monitored Incident Address Spent Funds",
            IncidentActivityType::WatchedTransactionConfirmed => "Incident Transaction Confirmed",
            IncidentActivityType::WatchedTransactionObserved => {
                "Incident Transaction Observed in Mempool"
            }
            IncidentActivityType::TransactionReplacement => "Incident Transaction Replaced (RBF)",
            IncidentActivityType::NewDescendantObserved => "New Incident Fund Descendant Observed",
        };

        TimelineEntry {
            id: Uuid::new_v4(),
            timestamp: activity.observed_at,
            title: title.to_string(),
            description: activity.description.clone(),
            category: TimelineCategory::OnChainMovement,
            source_id: None,
            evidence_ids: activity.evidence.clone(),
            transaction_txids: activity.trigger_txid.iter().cloned().collect(),
            block_heights: activity.block_height.into_iter().collect(),
            classification: activity.confidence,
        }
    }

    /// Generates an append-only IncidentUpdate.
    ///
    /// CRITICAL EPISTEMOLOGICAL RULE:
    /// Movement does NOT equal recovery. This method STRICTLY preserves
    /// the incident's existing `recovery` state without modifying `recovered_sats`.
    pub fn generate_incident_update(
        incident: &Incident,
        activity: &IncidentActivity,
    ) -> IncidentUpdate {
        IncidentUpdate {
            id: Uuid::new_v4(),
            timestamp: activity.observed_at,
            title: format!("On-Chain Movement: {:?}", activity.activity_type),
            summary: format!(
                "Observed on-chain activity ({:?}, confidence: {:?}, strength: {:?}) in tx {}. Recovery balance remains unchanged.",
                activity.activity_type,
                activity.confidence,
                activity.correlation_strength,
                activity.trigger_txid.as_deref().unwrap_or("unknown")
            ),
            source_id: None,
            // Strictly preserve the existing recovery summary; DO NOT alter recovered_sats!
            recovery_state: Some(incident.recovery.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obschain_core::{ObservationSource, TxInputObservation, TxOutputObservation};

    fn make_test_tx(
        txid: &str,
        inputs: Vec<TxInputObservation>,
        outputs: Vec<TxOutputObservation>,
    ) -> TransactionObservation {
        let total_in = inputs.iter().filter_map(|i| i.prev_out_value_sats).sum();
        let total_out = outputs.iter().map(|o| o.value_sats).sum();
        TransactionObservation {
            txid: txid.to_string(),
            timestamp: Utc::now(),
            block_hash: None,
            block_height: None,
            fee_sats: 10_000,
            size: 200,
            weight: 800,
            vsize: 200,
            fee_rate_sat_vb: Some(50.0),
            total_input_sats: total_in,
            total_output_sats: total_out,
            input_count: inputs.len(),
            output_count: outputs.len(),
            inputs,
            outputs,
            is_rbf: false,
            confirmed: false,
            source: Some(ObservationSource::mempool_ws(
                "wss://mempool.space/api/v1/ws",
            )),
        }
    }

    #[test]
    fn test_outpoint_match_and_mismatched_vout_rejection() {
        let mut engine = IncidentWatchEngine::with_follow_depth(3);
        let incident_id = Uuid::new_v4();

        // Monitored outpoint: tx001:0
        let target = WatchTarget::new_outpoint(
            incident_id,
            "OC-TEST-001",
            "tx001",
            0,
            ProvenanceClassification::OnChainVerified,
            "Bitcoin Ledger",
            Some("Target Outpoint".to_string()),
        );
        engine.add_target(target);

        // 1. Transaction spending tx001:1 (wrong vout) -> MUST NOT MATCH
        let tx_wrong_vout = make_test_tx(
            "tx_spend_1",
            vec![TxInputObservation {
                txid: "tx001".to_string(),
                vout: 1, // DIFFERENT VOUT!
                sequence: 0xffffffff,
                prev_out_value_sats: Some(50_000_000),
                prev_out_address: None,
                is_coinbase: false,
                historical_utxo: None,
            }],
            vec![],
        );
        let matches = engine.process_transaction(&tx_wrong_vout, None, None);
        assert!(matches.is_empty(), "Mismatched vout must be rejected");

        // 2. Transaction spending tx001:0 (exact outpoint) -> MUST MATCH
        let tx_exact = make_test_tx(
            "tx_spend_exact",
            vec![TxInputObservation {
                txid: "tx001".to_string(),
                vout: 0, // EXACT VOUT!
                sequence: 0xffffffff,
                prev_out_value_sats: Some(100_000_000_000), // 1,000 BTC
                prev_out_address: None,
                is_coinbase: false,
                historical_utxo: None,
            }],
            vec![TxOutputObservation {
                value_sats: 99_999_990_000,
                n: 0,
                script_pubkey_type: Some("v0_p2wpkh".to_string()),
                address: Some("bc1qdestination".to_string()),
                scriptpubkey_hex: Some("00141122334455667788990011223344556677889900".to_string()),
            }],
        );
        let matches = engine.process_transaction(&tx_exact, None, None);
        assert_eq!(matches.len(), 1, "Exact outpoint spend must match");
        let (act, alert) = &matches[0];
        assert_eq!(
            act.activity_type,
            IncidentActivityType::WatchedOutpointSpent
        );
        assert_eq!(act.correlation_strength, CorrelationStrength::Direct);
        assert_eq!(act.confidence, ProvenanceClassification::OnChainVerified);
        assert!(alert.is_some());
        assert_eq!(alert.as_ref().unwrap().severity, EventSeverity::Critical);
    }

    #[test]
    fn test_script_pubkey_match() {
        let mut engine = IncidentWatchEngine::with_follow_depth(3);
        let incident_id = Uuid::new_v4();
        let target_script = "0014abcdef0123456789abcdef0123456789abcdef01";

        let target = WatchTarget::new_script(
            incident_id,
            "OC-TEST-002",
            target_script,
            ProvenanceClassification::OnChainVerified,
            "Bitcoin Ledger",
            Some("Monitored Script".to_string()),
        );
        engine.add_target(target);

        let tx = make_test_tx(
            "tx_script_pay",
            vec![],
            vec![TxOutputObservation {
                value_sats: 500_000_000,
                n: 0,
                script_pubkey_type: Some("v0_p2wpkh".to_string()),
                address: None,
                scriptpubkey_hex: Some(target_script.to_string()),
            }],
        );

        let matches = engine.process_transaction(&tx, None, None);
        assert_eq!(matches.len(), 1);
        assert_eq!(
            matches[0].0.activity_type,
            IncidentActivityType::WatchedScriptReceived
        );
        assert_eq!(
            matches[0].0.correlation_strength,
            CorrelationStrength::Direct
        );
    }

    #[test]
    fn test_address_match_preserves_heuristic_boundary() {
        let mut engine = IncidentWatchEngine::with_follow_depth(3);
        let incident_id = Uuid::new_v4();

        // Heuristic target address
        let target = WatchTarget::new_address(
            incident_id,
            "OC-TEST-003",
            "bc1qclusteraddr000",
            ProvenanceClassification::Heuristic,
            "Cluster Analysis",
            Some("Heuristic Target".to_string()),
        );
        engine.add_target(target);

        let tx = make_test_tx(
            "tx_heur_spend",
            vec![TxInputObservation {
                txid: "tx_parent".to_string(),
                vout: 0,
                sequence: 0xffffffff,
                prev_out_value_sats: Some(20_000_000_000), // 200 BTC
                prev_out_address: Some("bc1qclusteraddr000".to_string()),
                is_coinbase: false,
                historical_utxo: None,
            }],
            vec![],
        );

        let matches = engine.process_transaction(&tx, None, None);
        assert_eq!(matches.len(), 1);
        let (act, alert) = &matches[0];
        assert_eq!(act.activity_type, IncidentActivityType::WatchedAddressSpent);
        // CRITICAL EPISTEMOLOGICAL INVARIANT: Heuristic stays Heuristic!
        assert_eq!(act.correlation_strength, CorrelationStrength::Heuristic);
        assert_eq!(act.confidence, ProvenanceClassification::Heuristic);

        // Heuristic alert severity is capped at Medium
        assert_eq!(alert.as_ref().unwrap().severity, EventSeverity::Medium);
    }

    #[test]
    fn test_descendant_tracking_bounded_depth() {
        let mut engine = IncidentWatchEngine::with_follow_depth(2); // Hard max depth: 2
        let incident_id = Uuid::new_v4();

        let origin = WatchTarget::new_outpoint(
            incident_id,
            "OC-TEST-004",
            "tx_origin",
            0,
            ProvenanceClassification::OnChainVerified,
            "Ledger",
            None,
        );
        engine.add_target(origin);

        // Hop 0 -> Hop 1: Spend origin outpoint
        let tx1 = make_test_tx(
            "tx_hop_1",
            vec![TxInputObservation {
                txid: "tx_origin".to_string(),
                vout: 0,
                sequence: 0xffffffff,
                prev_out_value_sats: Some(10_000_000),
                prev_out_address: None,
                is_coinbase: false,
                historical_utxo: None,
            }],
            vec![TxOutputObservation {
                value_sats: 9_990_000,
                n: 0,
                script_pubkey_type: Some("v0_p2wpkh".to_string()),
                address: Some("bc1qhop1".to_string()),
                scriptpubkey_hex: Some("00140101010101010101010101010101010101010101".to_string()),
            }],
        );
        let m1 = engine.process_transaction(&tx1, None, None);
        assert_eq!(m1.len(), 1);
        assert_eq!(
            m1[0].0.activity_type,
            IncidentActivityType::WatchedOutpointSpent
        );

        // Hop 1 -> Hop 2: Spend tx_hop_1:0 (Depth 1 descendant)
        let tx2 = make_test_tx(
            "tx_hop_2",
            vec![TxInputObservation {
                txid: "tx_hop_1".to_string(),
                vout: 0,
                sequence: 0xffffffff,
                prev_out_value_sats: Some(9_990_000),
                prev_out_address: None,
                is_coinbase: false,
                historical_utxo: None,
            }],
            vec![TxOutputObservation {
                value_sats: 9_980_000,
                n: 0,
                script_pubkey_type: Some("v0_p2wpkh".to_string()),
                address: Some("bc1qhop2".to_string()),
                scriptpubkey_hex: Some("00140202020202020202020202020202020202020202".to_string()),
            }],
        );
        let m2 = engine.process_transaction(&tx2, None, None);
        assert_eq!(m2.len(), 1);
        assert_eq!(
            m2[0].0.activity_type,
            IncidentActivityType::NewDescendantObserved
        );
        assert_eq!(
            m2[0].0.correlation_strength,
            CorrelationStrength::Structural
        );

        // Hop 2 -> Hop 3: Spend tx_hop_2:0 (Depth 2 descendant)
        let tx3 = make_test_tx(
            "tx_hop_3",
            vec![TxInputObservation {
                txid: "tx_hop_2".to_string(),
                vout: 0,
                sequence: 0xffffffff,
                prev_out_value_sats: Some(9_980_000),
                prev_out_address: None,
                is_coinbase: false,
                historical_utxo: None,
            }],
            vec![TxOutputObservation {
                value_sats: 9_970_000,
                n: 0,
                script_pubkey_type: Some("v0_p2wpkh".to_string()),
                address: Some("bc1qhop3".to_string()),
                scriptpubkey_hex: Some("00140303030303030303030303030303030303030303".to_string()),
            }],
        );
        let m3 = engine.process_transaction(&tx3, None, None);
        assert_eq!(m3.len(), 1);
        assert_eq!(
            m3[0].0.activity_type,
            IncidentActivityType::NewDescendantObserved
        );

        // Hop 3 -> Hop 4: Spend tx_hop_3:0 -> Bounded at depth 2, so Hop 4 MUST NOT match!
        let tx4 = make_test_tx(
            "tx_hop_4",
            vec![TxInputObservation {
                txid: "tx_hop_3".to_string(),
                vout: 0,
                sequence: 0xffffffff,
                prev_out_value_sats: Some(9_970_000),
                prev_out_address: None,
                is_coinbase: false,
                historical_utxo: None,
            }],
            vec![],
        );
        let m4 = engine.process_transaction(&tx4, None, None);
        assert!(
            m4.is_empty(),
            "Descendant tracking must stop at max_depth 2"
        );
    }

    #[test]
    fn test_incident_recovery_immutability() {
        let incident = obschain_incidents::create_liquid_2026_incident();
        let initial_recovered = incident.recovery.recovered_sats;
        let initial_affected = incident.recovery.affected_sats;

        let act = IncidentActivity {
            id: Uuid::new_v4(),
            incident_id: incident.id,
            case_id: incident.case_id.clone(),
            activity_type: IncidentActivityType::WatchedOutpointSpent,
            observed_at: Utc::now(),
            trigger_txid: Some("tx_moved".to_string()),
            block_height: None,
            block_hash: None,
            value_sats: Some(59_000_000_000),
            watch_target_id: Uuid::new_v4(),
            confidence: ProvenanceClassification::OnChainVerified,
            correlation_strength: CorrelationStrength::Direct,
            status: ActivityStatus::Mempool,
            source: ObservationSource::new("bitcoin", "mempool", None),
            evidence: vec![],
            description: "Movement observed".to_string(),
            details: None,
            dedup_key: "key".to_string(),
        };

        let update = IncidentWatchEngine::generate_incident_update(&incident, &act);
        let recovery = update.recovery_state.unwrap();

        // Recovery sats MUST NOT be mutated simply because coins moved!
        assert_eq!(recovery.recovered_sats, initial_recovered);
        assert_eq!(recovery.affected_sats, initial_affected);
    }
}
