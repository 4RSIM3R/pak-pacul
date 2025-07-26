use crate::{
    planner::expression::Expression,
    types::value::{DataType, Value},
};

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    Scan(ScanPlan),
    Filter(FilterPlan),
    Project(ProjectPlan),
    Join(JoinPlan),
    Aggregate(AggregatePlan),
    Insert(InsertPlan),
    Update(UpdatePlan),
    Delete(DeletePlan),
    CreateTable(CreateTablePlan),
    DropTable(DropTablePlan),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScanPlan {
    pub table_name: String,
    pub alias: Option<String>,
    pub projected_columns: Option<Vec<String>>, // naive one, because we don't have dedicated schema type
}

#[derive(Debug, Clone, PartialEq)]
pub struct FilterPlan {
    pub input: Box<LogicalPlan>,
    pub predicate: Expression,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectPlan {
    pub input: Box<LogicalPlan>,
    pub expressions: Vec<Expression>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JoinPlan {
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,
    pub join_type: JoinType,
    pub condition: Option<Expression>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggregatePlan {
    pub input: Box<LogicalPlan>,
    pub group_by: Vec<Expression>,
    pub aggregates: Vec<AggregateExpression>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggregateExpression {
    pub function: AggregateFunction,
    pub expression: Expression,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertPlan {
    pub table_name: String,
    pub columns: Option<Vec<String>>,
    pub values: Vec<Vec<Value>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdatePlan {
    pub table_name: String,
    pub assignments: Vec<Assignment>,
    pub condition: Option<Expression>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub column: String,
    pub value: Expression,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeletePlan {
    pub table_name: String,
    pub condition: Option<Expression>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTablePlan {
    pub table_name: String,
    pub columns: Vec<ColumnDefinition>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub primary_key: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropTablePlan {
    pub table_name: String,
    pub if_exists: bool,
}
