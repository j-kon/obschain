pub mod builder;
pub mod seed;
pub mod validation;

pub use builder::IncidentBuilder;
pub use seed::{
    create_liquid_2026_incident, load_liquid_2026_incident_from_json, LIQUID_2026_JSON,
    LIQUID_CASE_ID, LIQUID_INCIDENT_UUID,
};
pub use validation::{
    require_on_chain_backing, validate_evidence_claim, validate_fund_arithmetic, validate_incident,
    validate_incident_graph, validate_sender_identity_claim,
};
