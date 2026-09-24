use chrono::{Duration, TimeZone, Utc};
use obschain_core::{
    Chain, EventSeverity, Evidence, EvidenceType, GraphEdge, GraphEdgeType, GraphNode,
    GraphNodeType, IncidentBlock, IncidentGraph, IncidentStatus, IncidentTransaction,
    IncidentUpdate, OnChainMessage, ProvenanceClassification, RecoverySummary, Source,
    SourceCategory, TimelineCategory, TimelineEntry, TransactionRole,
};
use obschain_incidents::{
    load_liquid_2026_incident_from_json,
    validation::{
        validate_evidence_claim, validate_fund_arithmetic, validate_incident,
        validate_incident_graph, validate_sender_identity_claim,
    },
    IncidentBuilder,
};
use uuid::Uuid;

#[test]
fn test_incident_deserialization_from_json() {
    let incident = load_liquid_2026_incident_from_json().expect("Valid canonical JSON fixture");
    assert_eq!(incident.case_id, "OC-2026-0001");
    assert_eq!(incident.status, IncidentStatus::Monitoring);
    assert_eq!(incident.severity, EventSeverity::Critical);
    assert_eq!(incident.recovery.affected_sats, 399_602_000_000);
    assert_eq!(incident.recovery.recovered_sats, 340_000_000_000);
    assert_eq!(incident.recovery.outstanding_sats, 59_602_000_000);
    assert_eq!(incident.transactions.len(), 5);
    assert_eq!(incident.sources.len(), 4);
    assert_eq!(incident.evidence.len(), 4);
    assert!(!incident.graph.nodes.is_empty());
    assert!(!incident.graph.edges.is_empty());
}

#[test]
fn test_evidence_classification_hierarchy() {
    let classifications = vec![
        ProvenanceClassification::OnChainVerified,
        ProvenanceClassification::OfficiallyAttributed,
        ProvenanceClassification::ReputableReporting,
        ProvenanceClassification::Heuristic,
        ProvenanceClassification::Unverified,
        ProvenanceClassification::Disputed,
    ];
    for c in classifications {
        let serialized = serde_json::to_string(&c).expect("Serialize classification");
        let deserialized: ProvenanceClassification =
            serde_json::from_str(&serialized).expect("Deserialize classification");
        assert_eq!(c, deserialized);
    }
}

#[test]
fn test_timeline_ordering_enforcement() {
    let now = Utc::now();
    let earlier = now - Duration::hours(2);

    let t1 = TimelineEntry {
        id: Uuid::new_v4(),
        timestamp: now,
        title: "Later event".to_string(),
        description: "Happened second".to_string(),
        category: TimelineCategory::Exploit,
        classification: ProvenanceClassification::OnChainVerified,
        source_id: None,
        evidence_ids: vec![],
        transaction_txids: vec![],
        block_heights: vec![],
    };

    let t2 = TimelineEntry {
        id: Uuid::new_v4(),
        timestamp: earlier,
        title: "Earlier event".to_string(),
        description: "Happened first".to_string(),
        category: TimelineCategory::Discovery,
        classification: ProvenanceClassification::OfficiallyAttributed,
        source_id: None,
        evidence_ids: vec![],
        transaction_txids: vec![],
        block_heights: vec![],
    };

    let recovery = RecoverySummary::new(100_000_000, 50_000_000, now);

    // Timeline provided out of order
    let res = IncidentBuilder::new(
        "OC-TEST-01",
        "Timeline Test",
        "Testing timeline order",
        EventSeverity::High,
    )
    .status(IncidentStatus::Investigating)
    .recovery(recovery)
    .add_timeline_entry(t1)
    .add_timeline_entry(t2)
    .build();

    assert!(res.is_err(), "Builder must reject out-of-order timeline");
    let err = res.unwrap_err();
    assert!(
        err.to_string().contains("chronological"),
        "Error should mention chronological order: {err}"
    );
}

#[test]
fn test_transaction_references_and_roles() {
    let tx = IncidentTransaction {
        chain: Chain::Bitcoin,
        txid: "8db751a650ae2f12006b7e8c69a75e4df360e8afd6b9e05ae0b9fa6458a7b140".to_string(),
        role: TransactionRole::PegOut,
        amount_sats: Some(399_602_000_000),
        block_height: Some(965783),
        block_hash: None,
        confirmed_at: Some(Utc.with_ymd_and_hms(2026, 9, 6, 14, 28, 56).unwrap()),
        evidence_id: None,
        notes: Some("BTC released from Liquid federation".to_string()),
    };

    assert_eq!(tx.chain, Chain::Bitcoin);
    assert_eq!(tx.role, TransactionRole::PegOut);
    assert_eq!(tx.amount_sats, Some(399_602_000_000));
    assert_eq!(tx.block_height, Some(965783));
}

#[test]
fn test_block_references() {
    let block = IncidentBlock {
        chain: Chain::Bitcoin,
        height: 965783,
        hash: "0000000000000000000109f2b848cf998319f39446f254e0c4664bbdfc428d01".to_string(),
        timestamp: Utc.with_ymd_and_hms(2026, 9, 6, 14, 28, 56).unwrap(),
        tx_count: Some(3120),
        evidence_id: None,
    };

    assert_eq!(block.chain, Chain::Bitcoin);
    assert_eq!(block.height, 965783);
}

#[test]
fn test_fund_arithmetic_and_outstanding_calculation() {
    let now = Utc::now();
    let recovery = RecoverySummary::new(10_000_000, 3_000_000, now);

    assert_eq!(recovery.affected_sats, 10_000_000);
    assert_eq!(recovery.recovered_sats, 3_000_000);
    assert_eq!(recovery.outstanding_sats, 7_000_000);
    assert_eq!(recovery.recovery_percentage(), 30.0);

    // Test BTC conversion helpers
    assert_eq!(recovery.affected_btc(), 0.1);
    assert_eq!(recovery.recovered_btc(), 0.03);
    assert_eq!(recovery.outstanding_btc(), 0.07);

    // Test rejection when recovered exceeds affected
    let invalid = RecoverySummary {
        affected_sats: 1_000_000,
        recovered_sats: 2_000_000,
        outstanding_sats: 0,
        as_of_timestamp: now,
        source: None,
        is_estimate: false,
    };
    let test_incident = IncidentBuilder::new(
        "OC-TEST-FUND",
        "Fund Arithmetic Test",
        "Test invalid fund balance",
        EventSeverity::Low,
    )
    .recovery(invalid)
    .build_unvalidated();

    let res = validate_fund_arithmetic(&test_incident);
    assert!(res.is_err(), "Must reject recovered_sats > affected_sats");
}

#[test]
fn test_approximate_vs_exact_amounts() {
    let now = Utc::now();
    let exact_recovery =
        RecoverySummary::new(399_602_000_000, 340_000_000_000, now).with_estimate(false);
    assert!(!exact_recovery.is_estimate);

    let approx_recovery =
        RecoverySummary::new(400_000_000_000, 340_000_000_000, now).with_estimate(true);
    assert!(approx_recovery.is_estimate);
}

#[test]
fn test_source_provenance_retention() {
    let now = Utc::now();
    let source = Source {
        id: Uuid::new_v4(),
        publisher: "Blockstream".to_string(),
        title: "Liquid Network Security Incident Assessment".to_string(),
        url: Some("https://blockstream.com/liquid-incident".to_string()),
        publication_timestamp: Some(now),
        retrieved_timestamp: Some(now),
        source_category: SourceCategory::OfficialTechnicalReport,
        reliability_score: 0.95,
        name: Some("Blockstream Assessment".to_string()),
    };

    assert_eq!(source.publisher, "Blockstream");
    assert_eq!(
        source.source_category,
        SourceCategory::OfficialTechnicalReport
    );
    assert!(source.url.is_some());
    assert!(source.retrieved_timestamp.is_some());
}

#[test]
fn test_duplicate_evidence_rejection() {
    let now = Utc::now();
    let ev_id = Uuid::new_v4();
    let ev1 = Evidence {
        id: ev_id,
        incident_id: Uuid::new_v4(),
        evidence_type: EvidenceType::OnChainTransaction,
        confidence: ProvenanceClassification::OnChainVerified,
        title: "Evidence 1".to_string(),
        description: "Desc 1".to_string(),
        observed_at: Some(now),
        source_id: None,
        source_reference: None,
        txid: None,
        block_hash: None,
        block_height: None,
        chain: Chain::Bitcoin,
        verified: true,
        raw_data: None,
        created_at: now,
        reference: None,
        classification: Some(ProvenanceClassification::OnChainVerified),
    };
    let mut ev2 = ev1.clone();
    ev2.title = "Evidence 2 duplicate ID".to_string();

    let recovery = RecoverySummary::new(100_000, 50_000, now);
    let res = IncidentBuilder::new(
        "OC-TEST-DUP",
        "Dup Test",
        "Testing duplicate evidence rejection",
        EventSeverity::Medium,
    )
    .status(IncidentStatus::Investigating)
    .recovery(recovery)
    .add_evidence(ev1)
    .add_evidence(ev2)
    .build();

    assert!(res.is_err(), "Must reject duplicate evidence ID");
    assert!(res.unwrap_err().to_string().contains("Duplicate evidence"));
}

#[test]
fn test_graph_node_uniqueness() {
    let mut graph = IncidentGraph::default();
    graph.nodes.push(GraphNode {
        id: "tx:1".to_string(),
        label: "Tx 1".to_string(),
        node_type: GraphNodeType::Transaction,
        chain: Some(Chain::Bitcoin),
        metadata: None,
    });
    graph.nodes.push(GraphNode {
        id: "tx:1".to_string(),
        label: "Tx 1 Duplicate".to_string(),
        node_type: GraphNodeType::Transaction,
        chain: Some(Chain::Bitcoin),
        metadata: None,
    });

    let res = validate_incident_graph(&graph);
    assert!(res.is_err(), "Must reject duplicate graph node ID");
    assert!(res
        .unwrap_err()
        .to_string()
        .contains("Duplicate graph node"));
}

#[test]
fn test_graph_dangling_edge_rejection() {
    let mut graph = IncidentGraph::default();
    graph.nodes.push(GraphNode {
        id: "tx:1".to_string(),
        label: "Tx 1".to_string(),
        node_type: GraphNodeType::Transaction,
        chain: Some(Chain::Bitcoin),
        metadata: None,
    });
    // Edge references target "entity:nonexistent" which does not exist
    graph.edges.push(GraphEdge {
        source: "tx:1".to_string(),
        target: "entity:nonexistent".to_string(),
        relationship: GraphEdgeType::Spends,
        confidence: ProvenanceClassification::OnChainVerified,
    });

    let res = validate_incident_graph(&graph);
    assert!(res.is_err(), "Must reject dangling graph edge");
    assert!(res
        .unwrap_err()
        .to_string()
        .contains("does not exist in graph nodes"));
}

#[test]
fn test_incident_update_ordering_and_as_of_timestamps() {
    let t1 = Utc.with_ymd_and_hms(2026, 9, 7, 0, 0, 0).unwrap();
    let t2 = Utc.with_ymd_and_hms(2026, 9, 8, 0, 0, 0).unwrap();

    let u1 = IncidentUpdate {
        id: Uuid::new_v4(),
        timestamp: t1,
        title: "Interim patch deployed".to_string(),
        summary: "Emergency mitigation live".to_string(),
        source_id: None,
        recovery_state: None,
    };
    let u2 = IncidentUpdate {
        id: Uuid::new_v4(),
        timestamp: t2,
        title: "Hardened fix merged".to_string(),
        summary: "Elements PR 1600 merged".to_string(),
        source_id: None,
        recovery_state: None,
    };

    let recovery = RecoverySummary::new(100_000, 50_000, t1);
    let mut incident = IncidentBuilder::new(
        "OC-TEST-UPD",
        "Update Test",
        "Testing update order",
        EventSeverity::Low,
    )
    .status(IncidentStatus::Investigating)
    .recovery(recovery)
    .add_update(u1)
    .add_update(u2)
    .build()
    .expect("Valid incident");

    assert_eq!(incident.updates.len(), 2);
    assert!(incident.updates[0].timestamp <= incident.updates[1].timestamp);

    // Reverse updates to test validation
    incident.updates.reverse();
    let res = validate_incident(&incident);
    assert!(res.is_err(), "Must reject out-of-order updates");
    assert!(res.unwrap_err().to_string().contains("chronological order"));
}

#[test]
fn test_self_attributed_white_hat_message_cannot_become_verified_identity() {
    let msg = OnChainMessage {
        txid: "c103de95817b43f2df635ec6f35ff126ca26a7c6d20570c4b01866b2b3e69a19".to_string(),
        chain: Chain::Bitcoin,
        encoding: "utf-8".to_string(),
        decoded_text: "we are whitehats. contact us on chain.".to_string(),
        raw_hex: "776520617265207768697465686174732e20636f6e74616374207573206f6e20636861696e2e"
            .to_string(),
        confirmed_at: Some(Utc.with_ymd_and_hms(2026, 9, 6, 18, 30, 0).unwrap()),
        block_height: Some(965818),
        attributed_sender: Some("White Hat Hacker".to_string()),
        // Attributing sender identity as OnChainVerified MUST FAIL validation!
        sender_attribution_confidence: ProvenanceClassification::OnChainVerified,
    };

    let res = validate_sender_identity_claim(true, msg.sender_attribution_confidence);
    assert!(
        res.is_err(),
        "Must reject self-attributed white hat claim tagged as OnChainVerified"
    );
    let err = res.unwrap_err();
    assert!(
        err.to_string().contains("cannot be classified"),
        "Error must explain identity claim prohibition: {err}"
    );

    // But when tagged as Unverified or Heuristic, it is allowed!
    assert!(validate_sender_identity_claim(true, ProvenanceClassification::Unverified).is_ok());
    assert!(validate_sender_identity_claim(true, ProvenanceClassification::Heuristic).is_ok());
}

#[test]
fn test_heuristic_evidence_cannot_establish_definitive_ownership() {
    let now = Utc::now();
    let ev = Evidence {
        id: Uuid::new_v4(),
        incident_id: Uuid::new_v4(),
        evidence_type: EvidenceType::HeuristicCluster,
        confidence: ProvenanceClassification::Heuristic,
        title: "Actor definitive ownership of deposit address".to_string(),
        description: "Address 1xyz is owned by HackerGroup Alpha".to_string(),
        observed_at: Some(now),
        source_id: None,
        source_reference: None,
        txid: None,
        block_hash: None,
        block_height: None,
        chain: Chain::Bitcoin,
        verified: false,
        raw_data: None,
        created_at: now,
        reference: None,
        classification: Some(ProvenanceClassification::Heuristic),
    };

    let res = validate_evidence_claim(&ev, true);
    assert!(
        res.is_err(),
        "Heuristic evidence claiming definitive ownership must be rejected"
    );
    let err = res.unwrap_err();
    assert!(
        err.to_string().contains("cannot be made"),
        "Error message must specify ownership heuristic rule: {err}"
    );

    // When is_definitive_ownership_claim is false, heuristic evidence is permitted
    assert!(validate_evidence_claim(&ev, false).is_ok());

    // When confidence is OnChainVerified or OfficiallyAttributed, ownership claim is permitted
    let mut verified_ev = ev;
    verified_ev.confidence = ProvenanceClassification::OfficiallyAttributed;
    assert!(validate_evidence_claim(&verified_ev, true).is_ok());
}
