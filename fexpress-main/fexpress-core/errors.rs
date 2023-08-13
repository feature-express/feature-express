use thiserror::Error;

#[derive(Error, Debug)]
pub enum FeatureExpressError {
    #[error("Ingestion error: {0}")]
    IngestionError(String),
    #[error("Evaluation error: {0}")]
    EvaluationError(String),
    #[error("Other error: {0}")]
    OtherError(String),
}
