#[derive(Debug, thiserror::Error)]
pub enum PlannerError {
    #[error("SQL parsing error: {0}")]
    SqlParser(#[from] sqlparser::parser::ParserError),
    #[error("Unsupported statement: {0}")]
    UnsupportedStatement(String),
    #[error("Unsupported expression: {0}")]
    UnsupportedExpression(String),
    #[error("Invalid query structure: {0}")]
    InvalidQuery(String),
    #[error("Unsupported data type: {0}")]
    UnsupportedDataType(String),
}
