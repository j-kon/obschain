use thiserror::Error;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("Invalid observation data: {0}")]
    InvalidObservation(String),

    #[error("Validation failed: {0}")]
    ValidationError(String),

    #[error("Validation failed: {0}")]
    Validation(String),

    #[error("Provenance integrity violation: {0}")]
    ProvenanceViolation(String),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
}
