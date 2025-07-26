use crate::{
    planner::{error::PlannerError, logical_plan::LogicalPlan, operator::query::QueryOperator},
    types::value::DataType,
};
use sqlparser::{
    ast::{DataType as SqlDataType, Statement},
    dialect::SQLiteDialect,
    parser::Parser,
};

pub struct SqlParser;

impl SqlParser {
    pub fn new() -> Self {
        Self
    }

    pub fn parse_sql(&self, sql: &str) -> Result<LogicalPlan, PlannerError> {
        let dialect = SQLiteDialect {};
        let statements = Parser::parse_sql(&dialect, sql)?;

        if statements.len() != 1 {
            return Err(PlannerError::InvalidQuery(
                "Expected exactly one statement".to_string(),
            ));
        }

        self.to_plan(&statements[0])
    }

    fn to_plan(&self, statement: &Statement) -> Result<LogicalPlan, PlannerError> {
        match statement {
            Statement::Query(query) => QueryOperator::generate(query),
            _ => Err(PlannerError::UnsupportedStatement(format!("{:?}", statement))),
        }
    }

    fn convert_data_type(&self, sql_type: &SqlDataType) -> Result<DataType, PlannerError> {
        match sql_type {
            SqlDataType::Integer(_) => Ok(DataType::Integer),
            SqlDataType::Float(_) => Ok(DataType::Real),
            SqlDataType::Text => Ok(DataType::Text),
            SqlDataType::Boolean => Ok(DataType::Boolean),
            SqlDataType::Varchar(_) => Ok(DataType::Text),
            SqlDataType::Char(_) => Ok(DataType::Text),
            SqlDataType::Timestamp(_, _) => Ok(DataType::Timestamp),
            _ => Err(PlannerError::UnsupportedDataType(format!("{:?}", sql_type))),
        }
    }
}
