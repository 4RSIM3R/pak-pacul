use serde::{Deserialize, Serialize};

use crate::types::value::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BinaryOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    And,
    Or,
    Like,
    NotLike,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnRef {
    pub table: Option<String>,
    pub column: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BinaryOpExpression {
    pub left: Box<Expression>,
    pub operator: BinaryOperator,
    pub right: Box<Expression>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UnaryOpExpression {
    pub operator: UnaryOperator,
    pub expression: Box<Expression>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UnaryOperator {
    Not,
    Minus,
    Plus,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunctionExpression {
    pub name: String,
    pub args: Vec<Expression>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expression {
    Literal(Value),
    Column(ColumnRef),
    BinaryOp(BinaryOpExpression),
    UnaryOp(UnaryOpExpression),
    Function(FunctionExpression),
}

impl Expression {
    // Built-in functions
    pub fn upper(expr: Expression) -> Self {
        Expression::Function(FunctionExpression {
            name: "UPPER".to_string(),
            args: vec![expr],
        })
    }

    pub fn lower(expr: Expression) -> Self {
        Expression::Function(FunctionExpression {
            name: "LOWER".to_string(),
            args: vec![expr],
        })
    }

    // Helper constructors
    pub fn column(name: &str) -> Self {
        Expression::Column(ColumnRef {
            table: None,
            column: name.to_string(),
        })
    }

    pub fn literal(value: Value) -> Self {
        Expression::Literal(value)
    }

    pub fn eq(left: Expression, right: Expression) -> Self {
        Expression::BinaryOp(BinaryOpExpression {
            left: Box::new(left),
            operator: BinaryOperator::Equal,
            right: Box::new(right),
        })
    }
}
