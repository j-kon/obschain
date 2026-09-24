pub mod builder;
pub mod validation;

pub use builder::IncidentBuilder;
pub use validation::{require_on_chain_backing, validate_evidence_claim};
