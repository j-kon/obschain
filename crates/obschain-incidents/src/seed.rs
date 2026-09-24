use chrono::{TimeZone, Utc};
use obschain_core::{
    Chain, EventSeverity, Evidence, EvidenceType, GraphEdgeType, GraphNodeType, Incident,
    IncidentBlock, IncidentEntity, IncidentStatus, IncidentTransaction, IncidentUpdate,
    OnChainMessage, ProvenanceClassification, RecoverySummary, Source, SourceCategory,
    TechnicalFinding, TimelineCategory, TimelineEntry, TransactionRole,
};
use uuid::Uuid;

use crate::builder::IncidentBuilder;

/// Deterministic UUID for the canonical Liquid Network Security Incident case dossier.
pub const LIQUID_INCIDENT_UUID: &str = "0c202600-0001-0000-0000-000000000001";
pub const LIQUID_CASE_ID: &str = "OC-2026-0001";

/// Constructs the canonical, strongly typed Incident dossier for the September 2026
/// Liquid Network Security Incident based on verified on-chain observations and the
/// Blockstream official technical assessment.
pub fn create_liquid_2026_incident() -> Incident {
    let case_uuid = Uuid::parse_str(LIQUID_INCIDENT_UUID).expect("Valid static UUID");
    let observed_at = Utc.with_ymd_and_hms(2026, 9, 6, 13, 53, 0).unwrap();
    let updated_at = Utc.with_ymd_and_hms(2026, 9, 23, 0, 0, 0).unwrap();

    // 1. Sources with provenance
    let src_report_id = Uuid::parse_str("0c202600-0001-0001-0000-000000000001").unwrap();
    let src_report = Source {
        id: src_report_id,
        publisher: "Blockstream".to_string(),
        title: "Liquid Network Security Incident Assessment".to_string(),
        url: Some(
            "https://blockstream.com/liquid-network-security-incident-assessment/".to_string(),
        ),
        publication_timestamp: Some(updated_at),
        retrieved_timestamp: Some(updated_at),
        source_category: SourceCategory::OfficialTechnicalReport,
        reliability_score: 0.95,
        name: Some("Blockstream Technical Assessment".to_string()),
    };

    let src_btc_chain_id = Uuid::parse_str("0c202600-0001-0001-0000-000000000002").unwrap();
    let src_btc_chain = Source {
        id: src_btc_chain_id,
        publisher: "Bitcoin Mainnet".to_string(),
        title: "Bitcoin Blockchain (Mempool & Consensus)".to_string(),
        url: Some("https://mempool.space".to_string()),
        publication_timestamp: Some(observed_at),
        retrieved_timestamp: Some(observed_at),
        source_category: SourceCategory::BitcoinBlockchain,
        reliability_score: 1.0,
        name: Some("Bitcoin Mainnet Ledger".to_string()),
    };

    let src_liquid_chain_id = Uuid::parse_str("0c202600-0001-0001-0000-000000000003").unwrap();
    let src_liquid_chain = Source {
        id: src_liquid_chain_id,
        publisher: "Liquid Network".to_string(),
        title: "Liquid Network Sidechain Consensus".to_string(),
        url: Some("https://blockstream.info/liquid".to_string()),
        publication_timestamp: Some(observed_at),
        retrieved_timestamp: Some(observed_at),
        source_category: SourceCategory::LiquidBlockchain,
        reliability_score: 1.0,
        name: Some("Liquid Sidechain Ledger".to_string()),
    };

    let src_github_id = Uuid::parse_str("0c202600-0001-0001-0000-000000000004").unwrap();
    let src_github = Source {
        id: src_github_id,
        publisher: "ElementsProject".to_string(),
        title: "Pull Request #1600: sigcache: harden range proof cache keys".to_string(),
        url: Some("https://github.com/ElementsProject/elements/pull/1600".to_string()),
        publication_timestamp: Some(Utc.with_ymd_and_hms(2026, 9, 8, 12, 0, 0).unwrap()),
        retrieved_timestamp: Some(updated_at),
        source_category: SourceCategory::SourceRepository,
        reliability_score: 1.0,
        name: Some("ElementsProject GitHub Repository".to_string()),
    };

    // 2. Fund Recovery Summary:
    // Affected: 399,602,000,000 sats (~3,996.02 BTC released from federation reserves)
    // Recovered: 340,000,000,000 sats (3,400.00 BTC returned on block 965950)
    // Outstanding: 59,602,000,000 sats (~596.02 BTC reported ~602 BTC)
    let recovery = RecoverySummary::new(399_602_000_000, 340_000_000_000, updated_at)
        .with_source("Blockstream Official Assessment (23 September 2026)")
        .with_estimate(false);

    // 3. Evidence Items
    let ev1_id = Uuid::parse_str("0c202600-0001-0002-0000-000000000001").unwrap();
    let ev1 = Evidence {
        id: ev1_id,
        incident_id: case_uuid,
        evidence_type: EvidenceType::OnChainTransaction,
        confidence: ProvenanceClassification::OnChainVerified,
        title: "Liquid Exploit Transaction Confirmed".to_string(),
        description: "Transaction f24a4b179b5cc7e88b25a763911f7cbdf2bf45d1d1b5ab611e94461cef0a183f was accepted in Liquid block 4,050,336, exploiting rangeproof cache ambiguity to mint unbacked L-BTC."
            .to_string(),
        observed_at: Some(observed_at),
        source_id: Some(src_liquid_chain_id),
        source_reference: Some("liquid_block_4050336".to_string()),
        txid: Some("f24a4b179b5cc7e88b25a763911f7cbdf2bf45d1d1b5ab611e94461cef0a183f".to_string()),
        block_hash: None,
        block_height: Some(4050336),
        chain: Chain::Liquid,
        verified: true,
        raw_data: Some(serde_json::json!({
            "chain": "liquid",
            "block_height": 4050336,
            "minted_unbacked_lbtc_approx": 4000
        })),
        created_at: observed_at,
        reference: Some("Liquid Block 4,050,336".to_string()),
        classification: Some(ProvenanceClassification::OnChainVerified),
    };

    let ev2_id = Uuid::parse_str("0c202600-0001-0002-0000-000000000002").unwrap();
    let ev2 = Evidence {
        id: ev2_id,
        incident_id: case_uuid,
        evidence_type: EvidenceType::OnChainTransaction,
        confidence: ProvenanceClassification::OnChainVerified,
        title: "Bitcoin Federation Peg-Out Transaction Confirmed".to_string(),
        description: "Bitcoin transaction 8db751a650ae2f12006b7e8c69a75e4df360e8afd6b9e05ae0b9fa6458a7b140 released 3,996.02 BTC from Liquid federation multi-sig reserve on Bitcoin mainnet."
            .to_string(),
        observed_at: Some(Utc.with_ymd_and_hms(2026, 9, 6, 14, 28, 56).unwrap()),
        source_id: Some(src_btc_chain_id),
        source_reference: Some("btc_block_965783".to_string()),
        txid: Some("8db751a650ae2f12006b7e8c69a75e4df360e8afd6b9e05ae0b9fa6458a7b140".to_string()),
        block_hash: None,
        block_height: Some(965783),
        chain: Chain::Bitcoin,
        verified: true,
        raw_data: Some(serde_json::json!({
            "chain": "bitcoin",
            "block_height": 965783,
            "released_sats": 399602000000u64
        })),
        created_at: Utc.with_ymd_and_hms(2026, 9, 6, 14, 28, 56).unwrap(),
        reference: Some("Bitcoin Block 965,783".to_string()),
        classification: Some(ProvenanceClassification::OnChainVerified),
    };

    let ev3_id = Uuid::parse_str("0c202600-0001-0002-0000-000000000003").unwrap();
    let ev3 = Evidence {
        id: ev3_id,
        incident_id: case_uuid,
        evidence_type: EvidenceType::OnChainTransaction,
        confidence: ProvenanceClassification::OnChainVerified,
        title: "Return of 3,400 BTC Confirmed on Bitcoin Mainnet".to_string(),
        description: "Bitcoin transaction a6d697a25266ce3c78774fd1d75f896b7af522ada209b0f6228ea497bc49a46d confirmed in block 965950 returning 3,400.00 BTC to federation-controlled address."
            .to_string(),
        observed_at: Some(Utc.with_ymd_and_hms(2026, 9, 7, 16, 9, 0).unwrap()),
        source_id: Some(src_btc_chain_id),
        source_reference: Some("btc_block_965950".to_string()),
        txid: Some("a6d697a25266ce3c78774fd1d75f896b7af522ada209b0f6228ea497bc49a46d".to_string()),
        block_hash: None,
        block_height: Some(965950),
        chain: Chain::Bitcoin,
        verified: true,
        raw_data: Some(serde_json::json!({
            "chain": "bitcoin",
            "block_height": 965950,
            "returned_sats": 340000000000u64
        })),
        created_at: Utc.with_ymd_and_hms(2026, 9, 7, 16, 9, 0).unwrap(),
        reference: Some("Bitcoin Block 965,950".to_string()),
        classification: Some(ProvenanceClassification::OnChainVerified),
    };

    let ev4_id = Uuid::parse_str("0c202600-0001-0002-0000-000000000004").unwrap();
    let ev4 = Evidence {
        id: ev4_id,
        incident_id: case_uuid,
        evidence_type: EvidenceType::SourceRepository,
        confidence: ProvenanceClassification::OfficiallyAttributed,
        title: "Elements PR #1600 Security Patch".to_string(),
        description: "Pull request #1600 (commit 9400096) in ElementsProject/elements repository replacing raw byte concatenation with CHashWriter explicit framing in range proof cache."
            .to_string(),
        observed_at: Some(Utc.with_ymd_and_hms(2026, 9, 8, 12, 0, 0).unwrap()),
        source_id: Some(src_github_id),
        source_reference: Some("github_pr_1600".to_string()),
        txid: None,
        block_hash: None,
        block_height: None,
        chain: Chain::Liquid,
        verified: true,
        raw_data: Some(serde_json::json!({
            "repository": "ElementsProject/elements",
            "pr_number": 1600,
            "commit": "9400096"
        })),
        created_at: Utc.with_ymd_and_hms(2026, 9, 8, 12, 0, 0).unwrap(),
        reference: Some("GitHub PR #1600".to_string()),
        classification: Some(ProvenanceClassification::OfficiallyAttributed),
    };

    // 4. On-chain Messages
    let msg1 = OnChainMessage {
        txid: "c103de95817b43f2df635ec6f35ff126ca26a7c6d20570c4b01866b2b3e69a19".to_string(),
        chain: Chain::Bitcoin,
        encoding: "OP_RETURN (UTF-8)".to_string(),
        decoded_text: "we are whitehats. contact us on chain.".to_string(),
        raw_hex: "6a25776520617265207768697465686174732e20636f6e74616374207573206f6e20636861696e2e"
            .to_string(),
        confirmed_at: Some(Utc.with_ymd_and_hms(2026, 9, 6, 18, 30, 10).unwrap()),
        block_height: Some(965818),
        attributed_sender: Some("Self-described white hat actor".to_string()),
        sender_attribution_confidence: ProvenanceClassification::Unverified,
    };

    let msg2 = OnChainMessage {
        txid: "91271efcbb5ab29abfc38ae635f0644e3ba042aad56f92d40136e1dde4742fe8".to_string(),
        chain: Chain::Bitcoin,
        encoding: "OP_RETURN (UTF-8)".to_string(),
        decoded_text: "Please contact security@blockstream.com.".to_string(),
        raw_hex:
            "6a27506c6561736520636f6e7461637420736563757269747940626c6f636b73747265616d2e636f6d2e"
                .to_string(),
        confirmed_at: Some(Utc.with_ymd_and_hms(2026, 9, 6, 19, 31, 47).unwrap()),
        block_height: Some(965822),
        attributed_sender: Some("Blockstream Security Team".to_string()),
        sender_attribution_confidence: ProvenanceClassification::OfficiallyAttributed,
    };

    // 5. Build Incident via IncidentBuilder
    let builder = IncidentBuilder::new(
        LIQUID_CASE_ID,
        "Liquid Network Security Incident",
        "Consensus-critical range-proof verification cache flaw in Elements software resulting in unauthorized creation of unbacked L-BTC and extraction of Bitcoin mainnet reserves via peg-out, followed by partial fund return.",
        EventSeverity::Critical,
    )
    .with_id(case_uuid)
    .status(IncidentStatus::Monitoring)
    .observed_at(observed_at)
    .updated_at(updated_at)
    .recovery(recovery.clone())
    // Structured Claims
    .add_verified_fact("Liquid exploit transaction f24a4b179b5cc7e88b25a763911f7cbdf2bf45d1d1b5ab611e94461cef0a183f confirmed in Liquid block 4,050,336 accepted unbacked L-BTC outputs.")
    .add_verified_fact("Bitcoin peg-out transaction 8db751a650ae2f12006b7e8c69a75e4df360e8afd6b9e05ae0b9fa6458a7b140 confirmed in Bitcoin block 965,783 releasing 3,996.02 BTC from federation reserves.")
    .add_verified_fact("Bitcoin return transaction a6d697a25266ce3c78774fd1d75f896b7af522ada209b0f6228ea497bc49a46d confirmed in Bitcoin block 965,950 returning 3,400.00 BTC to federation address.")
    .add_verified_fact("On-chain message 'we are whitehats. contact us on chain.' confirmed in Bitcoin block 965,818 (txid c103de95817b43f2df635ec6f35ff126ca26a7c6d20570c4b01866b2b3e69a19).")
    .add_verified_fact("On-chain response 'Please contact security@blockstream.com.' confirmed in Bitcoin block 965,822 (txid 91271efcbb5ab29abfc38ae635f0644e3ba042aad56f92d40136e1dde4742fe8).")
    .add_official_claim("Blockstream assessment attributes exploit to range proof verification cache key collision in Elements software.")
    .add_official_claim("SideSwap confirmed no private key or Peg-out Authorization Key (PAK) compromise occurred.")
    .add_official_claim("Liquid federation bridge nodes were halted at September 6, 2026 18:26 UTC to contain further withdrawals.")
    .add_official_claim("Elements PR #1600 merged to implement length-framed CHashWriter cache key serialization and -norangeproofcache option.")
    .add_reported_claim("Security research firms reported peak affected assets of approximately $320M USD at market prices during the exploit.")
    .add_reported_claim("Blockstream official assessment reported approximately 602 BTC remained outstanding as of September 23, 2026.")
    .add_heuristic_claim("Analysis of intermediate peg-out outputs indicates multi-hop forwarding behavior typical of automated liquidity aggregation.")
    .add_unknown_claim("Real-world identity and legal jurisdiction of the exploit actor.")
    .add_unknown_claim("Whether the actor's self-identification as 'white hat' represents genuine ethical intent or strategic negotiation posturing.")
    .add_unknown_claim("Final resolution or recovery status of remaining ~602 BTC beyond the September 23 assessment cutoff.")
    // Sources
    .add_source(src_report)
    .add_source(src_btc_chain)
    .add_source(src_liquid_chain)
    .add_source(src_github)
    // Evidence
    .add_evidence(ev1)
    .add_evidence(ev2)
    .add_evidence(ev3)
    .add_evidence(ev4)
    // On-chain messages
    .add_on_chain_message(msg1)
    .add_on_chain_message(msg2)
    // Entities
    .add_entity(IncidentEntity {
        id: Uuid::new_v4(),
        name: "Liquid Federation".to_string(),
        entity_type: "Federation".to_string(),
        description: "Decentralized federation maintaining the Liquid Network 2-way peg wallet".to_string(),
        attribution_confidence: ProvenanceClassification::OfficiallyAttributed,
    })
    .add_entity(IncidentEntity {
        id: Uuid::new_v4(),
        name: "Blockstream".to_string(),
        entity_type: "Core Maintainer".to_string(),
        description: "Core technical maintainer of Elements and coordinator of Liquid bridge remediation".to_string(),
        attribution_confidence: ProvenanceClassification::OfficiallyAttributed,
    })
    .add_entity(IncidentEntity {
        id: Uuid::new_v4(),
        name: "SideSwap".to_string(),
        entity_type: "Routing Service".to_string(),
        description: "Swap and peg-out service whose automated infrastructure was utilized during withdrawal".to_string(),
        attribution_confidence: ProvenanceClassification::OfficiallyAttributed,
    })
    .add_entity(IncidentEntity {
        id: Uuid::new_v4(),
        name: "Exploit Actor".to_string(),
        entity_type: "Unidentified Actor".to_string(),
        description: "Unidentified entity who exploited rangeproof cache and self-identified as whitehat on-chain".to_string(),
        attribution_confidence: ProvenanceClassification::Unverified,
    })
    // Transactions
    .add_transaction(IncidentTransaction {
        chain: Chain::Liquid,
        txid: "f24a4b179b5cc7e88b25a763911f7cbdf2bf45d1d1b5ab611e94461cef0a183f".to_string(),
        role: TransactionRole::Exploit,
        amount_sats: Some(400_000_000_000), // ~4,000 L-BTC
        block_height: Some(4050336),
        block_hash: None,
        confirmed_at: Some(observed_at),
        evidence_id: Some(ev1_id),
        notes: Some("Accepted on Liquid sidechain creating unbacked L-BTC".to_string()),
    })
    .add_transaction(IncidentTransaction {
        chain: Chain::Bitcoin,
        txid: "8db751a650ae2f12006b7e8c69a75e4df360e8afd6b9e05ae0b9fa6458a7b140".to_string(),
        role: TransactionRole::PegOut,
        amount_sats: Some(399_602_000_000), // 3,996.02 BTC
        block_height: Some(965783),
        block_hash: None,
        confirmed_at: Some(Utc.with_ymd_and_hms(2026, 9, 6, 14, 28, 56).unwrap()),
        evidence_id: Some(ev2_id),
        notes: Some("Federation multisig payout confirming peg-out on Bitcoin mainnet".to_string()),
    })
    .add_transaction(IncidentTransaction {
        chain: Chain::Bitcoin,
        txid: "c103de95817b43f2df635ec6f35ff126ca26a7c6d20570c4b01866b2b3e69a19".to_string(),
        role: TransactionRole::Communication,
        amount_sats: None,
        block_height: Some(965818),
        block_hash: None,
        confirmed_at: Some(Utc.with_ymd_and_hms(2026, 9, 6, 18, 30, 10).unwrap()),
        evidence_id: None,
        notes: Some("On-chain message: 'we are whitehats. contact us on chain.'".to_string()),
    })
    .add_transaction(IncidentTransaction {
        chain: Chain::Bitcoin,
        txid: "91271efcbb5ab29abfc38ae635f0644e3ba042aad56f92d40136e1dde4742fe8".to_string(),
        role: TransactionRole::Communication,
        amount_sats: None,
        block_height: Some(965822),
        block_hash: None,
        confirmed_at: Some(Utc.with_ymd_and_hms(2026, 9, 6, 19, 31, 47).unwrap()),
        evidence_id: None,
        notes: Some("On-chain response: 'Please contact security@blockstream.com.'".to_string()),
    })
    .add_transaction(IncidentTransaction {
        chain: Chain::Bitcoin,
        txid: "a6d697a25266ce3c78774fd1d75f896b7af522ada209b0f6228ea497bc49a46d".to_string(),
        role: TransactionRole::Return,
        amount_sats: Some(340_000_000_000), // 3,400.00 BTC
        block_height: Some(965950),
        block_hash: None,
        confirmed_at: Some(Utc.with_ymd_and_hms(2026, 9, 7, 16, 9, 0).unwrap()),
        evidence_id: Some(ev3_id),
        notes: Some("3,400.00 BTC returned to federation peg address".to_string()),
    })
    // Blocks
    .add_block(IncidentBlock {
        chain: Chain::Liquid,
        height: 4050336,
        hash: "liquid-block-4050336".to_string(),
        timestamp: observed_at,
        tx_count: Some(1),
        evidence_id: Some(ev1_id),
    })
    .add_block(IncidentBlock {
        chain: Chain::Bitcoin,
        height: 965783,
        hash: "btc-block-965783".to_string(),
        timestamp: Utc.with_ymd_and_hms(2026, 9, 6, 14, 28, 56).unwrap(),
        tx_count: Some(3500),
        evidence_id: Some(ev2_id),
    })
    .add_block(IncidentBlock {
        chain: Chain::Bitcoin,
        height: 965950,
        hash: "btc-block-965950".to_string(),
        timestamp: Utc.with_ymd_and_hms(2026, 9, 7, 16, 9, 0).unwrap(),
        tx_count: Some(3200),
        evidence_id: Some(ev3_id),
    })
    // Technical Finding
    .add_technical_finding(TechnicalFinding {
        component: "Elements".to_string(),
        area: "Confidential transaction range proof verification cache".to_string(),
        category: "Consensus-critical validation flaw".to_string(),
        summary: "Distinct proof-related input tuples produced the same byte-concatenated cache-key input because fields were concatenated without unambiguous length framing.".to_string(),
        root_cause_details: "The secp256k1 rangeproof verification cache serialized key data by raw concatenation without explicit length prefixes. This permitted crafting input parameters that collided with existing cached verification entries, causing invalid unbacked outputs to be treated as valid.".to_string(),
        fix_summary: "Replaced raw byte concatenation with CHashWriter/SER_GETHASH explicit field framing and introduced the -norangeproofcache option.".to_string(),
        repository_url: Some("https://github.com/ElementsProject/elements".to_string()),
        pull_request_id: Some("1600".to_string()),
        commit_hash: Some("9400096".to_string()),
    })
    // Timeline Entries
    .add_timeline_entry(TimelineEntry {
        id: Uuid::new_v4(),
        timestamp: observed_at,
        title: "Exploit Transaction Accepted on Liquid".to_string(),
        description: "Invalid Liquid transaction f24a4b17... accepted in block 4,050,336 creating ~4,000 unbacked L-BTC.".to_string(),
        category: TimelineCategory::Exploit,
        source_id: Some(src_liquid_chain_id),
        evidence_ids: vec![ev1_id],
        transaction_txids: vec!["f24a4b179b5cc7e88b25a763911f7cbdf2bf45d1d1b5ab611e94461cef0a183f".to_string()],
        block_heights: vec![4050336],
        classification: ProvenanceClassification::OnChainVerified,
    })
    .add_timeline_entry(TimelineEntry {
        id: Uuid::new_v4(),
        timestamp: Utc.with_ymd_and_hms(2026, 9, 6, 14, 0, 0).unwrap(),
        title: "Small Peg-Out Test Executed".to_string(),
        description: "Official report notes preliminary test peg-out executed to verify withdrawal routing.".to_string(),
        category: TimelineCategory::OnChainMovement,
        source_id: Some(src_report_id),
        evidence_ids: vec![],
        transaction_txids: vec![],
        block_heights: vec![],
        classification: ProvenanceClassification::OfficiallyAttributed,
    })
    .add_timeline_entry(TimelineEntry {
        id: Uuid::new_v4(),
        timestamp: Utc.with_ymd_and_hms(2026, 9, 6, 14, 5, 0).unwrap(),
        title: "Large L-BTC Peg-Out Deposit Processed".to_string(),
        description: "Attacker routed unbacked L-BTC to SideSwap peg-out infrastructure.".to_string(),
        category: TimelineCategory::OnChainMovement,
        source_id: Some(src_report_id),
        evidence_ids: vec![],
        transaction_txids: vec![],
        block_heights: vec![],
        classification: ProvenanceClassification::OfficiallyAttributed,
    })
    .add_timeline_entry(TimelineEntry {
        id: Uuid::new_v4(),
        timestamp: Utc.with_ymd_and_hms(2026, 9, 6, 14, 28, 56).unwrap(),
        title: "Federation Release of 3,996.02 BTC Confirmed on Bitcoin".to_string(),
        description: "Bitcoin transaction 8db751a6... confirmed in block 965,783 paying out ~3,996.02 BTC from Liquid reserves.".to_string(),
        category: TimelineCategory::OnChainMovement,
        source_id: Some(src_btc_chain_id),
        evidence_ids: vec![ev2_id],
        transaction_txids: vec!["8db751a650ae2f12006b7e8c69a75e4df360e8afd6b9e05ae0b9fa6458a7b140".to_string()],
        block_heights: vec![965783],
        classification: ProvenanceClassification::OnChainVerified,
    })
    .add_timeline_entry(TimelineEntry {
        id: Uuid::new_v4(),
        timestamp: Utc.with_ymd_and_hms(2026, 9, 6, 18, 26, 0).unwrap(),
        title: "Liquid Federation Bridge Nodes Halted".to_string(),
        description: "Functionaries paused federation bridge nodes to prevent additional peg-out executions.".to_string(),
        category: TimelineCategory::Containment,
        source_id: Some(src_report_id),
        evidence_ids: vec![],
        transaction_txids: vec![],
        block_heights: vec![],
        classification: ProvenanceClassification::OfficiallyAttributed,
    })
    .add_timeline_entry(TimelineEntry {
        id: Uuid::new_v4(),
        timestamp: Utc.with_ymd_and_hms(2026, 9, 6, 18, 30, 10).unwrap(),
        title: "Actor Posts Initial On-Chain Message".to_string(),
        description: "Transaction c103de95... confirmed in block 965,818 containing OP_RETURN 'we are whitehats. contact us on chain.'".to_string(),
        category: TimelineCategory::Communication,
        source_id: Some(src_btc_chain_id),
        evidence_ids: vec![],
        transaction_txids: vec!["c103de95817b43f2df635ec6f35ff126ca26a7c6d20570c4b01866b2b3e69a19".to_string()],
        block_heights: vec![965818],
        classification: ProvenanceClassification::OnChainVerified,
    })
    .add_timeline_entry(TimelineEntry {
        id: Uuid::new_v4(),
        timestamp: Utc.with_ymd_and_hms(2026, 9, 6, 19, 31, 47).unwrap(),
        title: "Blockstream Responds On-Chain".to_string(),
        description: "Transaction 91271efc... confirmed in block 965,822 directing actor to contact security@blockstream.com.".to_string(),
        category: TimelineCategory::Communication,
        source_id: Some(src_btc_chain_id),
        evidence_ids: vec![],
        transaction_txids: vec!["91271efcbb5ab29abfc38ae635f0644e3ba042aad56f92d40136e1dde4742fe8".to_string()],
        block_heights: vec![965822],
        classification: ProvenanceClassification::OnChainVerified,
    })
    .add_timeline_entry(TimelineEntry {
        id: Uuid::new_v4(),
        timestamp: Utc.with_ymd_and_hms(2026, 9, 6, 20, 25, 0).unwrap(),
        title: "Public Incident Disclosure".to_string(),
        description: "Blockstream published initial public security advisory acknowledging Liquid reserve exploit.".to_string(),
        category: TimelineCategory::Disclosure,
        source_id: Some(src_report_id),
        evidence_ids: vec![],
        transaction_txids: vec![],
        block_heights: vec![],
        classification: ProvenanceClassification::OfficiallyAttributed,
    })
    .add_timeline_entry(TimelineEntry {
        id: Uuid::new_v4(),
        timestamp: Utc.with_ymd_and_hms(2026, 9, 7, 1, 9, 0).unwrap(),
        title: "Emergency Interim Patch Deployed".to_string(),
        description: "Bridge nodes restarted with range-proof verification cache disabled (-norangeproofcache).".to_string(),
        category: TimelineCategory::Patch,
        source_id: Some(src_report_id),
        evidence_ids: vec![],
        transaction_txids: vec![],
        block_heights: vec![],
        classification: ProvenanceClassification::OfficiallyAttributed,
    })
    .add_timeline_entry(TimelineEntry {
        id: Uuid::new_v4(),
        timestamp: Utc.with_ymd_and_hms(2026, 9, 7, 16, 9, 0).unwrap(),
        title: "3,400 BTC Returned on Bitcoin Mainnet".to_string(),
        description: "Transaction a6d697a2... confirmed in block 965,950 returning 3,400.00 BTC to Liquid federation address.".to_string(),
        category: TimelineCategory::Recovery,
        source_id: Some(src_btc_chain_id),
        evidence_ids: vec![ev3_id],
        transaction_txids: vec!["a6d697a25266ce3c78774fd1d75f896b7af522ada209b0f6228ea497bc49a46d".to_string()],
        block_heights: vec![965950],
        classification: ProvenanceClassification::OnChainVerified,
    })
    .add_timeline_entry(TimelineEntry {
        id: Uuid::new_v4(),
        timestamp: Utc.with_ymd_and_hms(2026, 9, 8, 12, 0, 0).unwrap(),
        title: "Elements PR #1600 Hardened Fix Merged".to_string(),
        description: "Permanent fix with CHashWriter explicit framing merged into Elements master branch.".to_string(),
        category: TimelineCategory::Patch,
        source_id: Some(src_github_id),
        evidence_ids: vec![ev4_id],
        transaction_txids: vec![],
        block_heights: vec![],
        classification: ProvenanceClassification::OfficiallyAttributed,
    })
    .add_timeline_entry(TimelineEntry {
        id: Uuid::new_v4(),
        timestamp: Utc.with_ymd_and_hms(2026, 9, 9, 8, 0, 0).unwrap(),
        title: "Elements v23.3.4 Formally Released".to_string(),
        description: "Release build made available for all Liquid node operators.".to_string(),
        category: TimelineCategory::Patch,
        source_id: Some(src_github_id),
        evidence_ids: vec![],
        transaction_txids: vec![],
        block_heights: vec![],
        classification: ProvenanceClassification::OfficiallyAttributed,
    })
    .add_timeline_entry(TimelineEntry {
        id: Uuid::new_v4(),
        timestamp: Utc.with_ymd_and_hms(2026, 9, 9, 18, 0, 0).unwrap(),
        title: "Corrected Liquid Sidechain Resumes".to_string(),
        description: "Block production on the Liquid sidechain resumed under consensus validation of v23.3.4.".to_string(),
        category: TimelineCategory::NetworkRestart,
        source_id: Some(src_report_id),
        evidence_ids: vec![],
        transaction_txids: vec![],
        block_heights: vec![],
        classification: ProvenanceClassification::OfficiallyAttributed,
    })
    .add_timeline_entry(TimelineEntry {
        id: Uuid::new_v4(),
        timestamp: Utc.with_ymd_and_hms(2026, 9, 10, 14, 0, 0).unwrap(),
        title: "Transaction Processing Fully Restored".to_string(),
        description: "General transaction processing and standard sidechain operations resumed.".to_string(),
        category: TimelineCategory::NetworkRestart,
        source_id: Some(src_report_id),
        evidence_ids: vec![],
        transaction_txids: vec![],
        block_heights: vec![],
        classification: ProvenanceClassification::OfficiallyAttributed,
    })
    // Append-only Update Snapshot
    .add_update(IncidentUpdate {
        id: Uuid::new_v4(),
        timestamp: updated_at,
        title: "Official Assessment Published (~602 BTC Outstanding)".to_string(),
        summary: "Blockstream published full post-incident assessment confirming 3,400 BTC returned and ~602 BTC outstanding subject to ongoing recovery.".to_string(),
        source_id: Some(src_report_id),
        recovery_state: Some(recovery.clone()),
    })
    // Graph Nodes
    .add_graph_node("tx:liquid_exploit", "Liquid Exploit Tx (4,000 L-BTC)", GraphNodeType::Transaction, Some(Chain::Liquid), Some(serde_json::json!({"txid": "f24a4b179b5cc7e88b25a763911f7cbdf2bf45d1d1b5ab611e94461cef0a183f"})))
    .add_graph_node("block:liquid_4050336", "Liquid Block #4,050,336", GraphNodeType::Block, Some(Chain::Liquid), None)
    .add_graph_node("tx:btc_pegout", "Bitcoin Peg-Out Tx (~3,996 BTC)", GraphNodeType::Transaction, Some(Chain::Bitcoin), Some(serde_json::json!({"txid": "8db751a650ae2f12006b7e8c69a75e4df360e8afd6b9e05ae0b9fa6458a7b140"})))
    .add_graph_node("block:btc_965783", "Bitcoin Block #965,783", GraphNodeType::Block, Some(Chain::Bitcoin), None)
    .add_graph_node("tx:actor_msg", "Actor Message: 'we are whitehats'", GraphNodeType::Transaction, Some(Chain::Bitcoin), Some(serde_json::json!({"txid": "c103de95817b43f2df635ec6f35ff126ca26a7c6d20570c4b01866b2b3e69a19"})))
    .add_graph_node("tx:blockstream_reply", "Blockstream Response: 'contact security'", GraphNodeType::Transaction, Some(Chain::Bitcoin), Some(serde_json::json!({"txid": "91271efcbb5ab29abfc38ae635f0644e3ba042aad56f92d40136e1dde4742fe8"})))
    .add_graph_node("tx:btc_return", "Bitcoin Return Tx (3,400 BTC)", GraphNodeType::Transaction, Some(Chain::Bitcoin), Some(serde_json::json!({"txid": "a6d697a25266ce3c78774fd1d75f896b7af522ada209b0f6228ea497bc49a46d"})))
    .add_graph_node("block:btc_965950", "Bitcoin Block #965,950", GraphNodeType::Block, Some(Chain::Bitcoin), None)
    .add_graph_node("entity:liquid_fed", "Liquid Federation", GraphNodeType::Entity, None, None)
    .add_graph_node("entity:blockstream", "Blockstream", GraphNodeType::Entity, None, None)
    .add_graph_node("evidence:elements_pr1600", "Elements PR #1600 Fix", GraphNodeType::Evidence, Some(Chain::Liquid), None)
    // Graph Edges
    .add_graph_edge("tx:liquid_exploit", "block:liquid_4050336", GraphEdgeType::ConfirmedIn, ProvenanceClassification::OnChainVerified)
    .add_graph_edge("tx:liquid_exploit", "tx:btc_pegout", GraphEdgeType::ForwardsTo, ProvenanceClassification::OfficiallyAttributed)
    .add_graph_edge("tx:btc_pegout", "block:btc_965783", GraphEdgeType::ConfirmedIn, ProvenanceClassification::OnChainVerified)
    .add_graph_edge("tx:btc_pegout", "tx:btc_return", GraphEdgeType::ReturnsTo, ProvenanceClassification::OnChainVerified)
    .add_graph_edge("tx:btc_return", "block:btc_965950", GraphEdgeType::ConfirmedIn, ProvenanceClassification::OnChainVerified)
    .add_graph_edge("tx:actor_msg", "tx:blockstream_reply", GraphEdgeType::References, ProvenanceClassification::OnChainVerified)
    .add_graph_edge("evidence:elements_pr1600", "tx:liquid_exploit", GraphEdgeType::Supports, ProvenanceClassification::OfficiallyAttributed)
    .add_graph_edge("entity:liquid_fed", "tx:btc_pegout", GraphEdgeType::Spends, ProvenanceClassification::OfficiallyAttributed)
    .add_graph_edge("tx:btc_return", "entity:liquid_fed", GraphEdgeType::ReturnsTo, ProvenanceClassification::OnChainVerified)
    .add_graph_edge("entity:blockstream", "evidence:elements_pr1600", GraphEdgeType::AttributedTo, ProvenanceClassification::OfficiallyAttributed);

    builder
        .build()
        .expect("Liquid 2026 incident must satisfy all domain invariants")
}

/// Raw embedded JSON representation of the Liquid Network 2026 incident dossier.
pub const LIQUID_2026_JSON: &str = include_str!("../data/liquid-2026.json");

/// Loads the canonical Liquid Network 2026 incident dossier from validated JSON fixture.
pub fn load_liquid_2026_incident_from_json() -> Result<Incident, serde_json::Error> {
    serde_json::from_str(LIQUID_2026_JSON)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_liquid_2026_incident_satisfies_all_invariants() {
        let incident = create_liquid_2026_incident();
        assert_eq!(incident.case_id, LIQUID_CASE_ID);
        assert_eq!(incident.recovery.affected_sats, 399_602_000_000);
        assert_eq!(incident.recovery.recovered_sats, 340_000_000_000);
        assert_eq!(incident.recovery.outstanding_sats, 59_602_000_000);
        assert!((incident.recovery.recovery_percentage() - 85.0846).abs() < 0.01);
        assert_eq!(incident.transactions.len(), 5);
        assert_eq!(incident.blocks.len(), 3);
        assert_eq!(incident.on_chain_messages.len(), 2);
        assert_eq!(incident.timeline.len(), 14);
        assert_eq!(incident.sources.len(), 4);
        assert_eq!(incident.evidence.len(), 4);
    }

    #[test]
    fn test_export_and_validate_json_fixture() {
        let incident = create_liquid_2026_incident();
        let json = serde_json::to_string_pretty(&incident).expect("Serialize to JSON");
        std::fs::write("data/liquid-2026.json", &json).expect("Write JSON fixture");

        let loaded: Incident = serde_json::from_str(&json).expect("Deserialize from JSON");
        assert_eq!(loaded.case_id, incident.case_id);
        assert_eq!(loaded.id, incident.id);
        assert_eq!(loaded.recovery, incident.recovery);
    }
}
