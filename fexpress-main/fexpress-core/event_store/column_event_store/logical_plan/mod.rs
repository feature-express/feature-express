use crate::ast::core::{AggregateFunction, BExpr, Expr, HavingExprType};

// the idea is to merge queries but only until some step.
// for example we can merge the expressions only if they share the same root
// to simplify things I would only consider merging until projection.
pub enum QueryOperation {
    // Represents the scanning of a table
    FullTableScan {
        table: String,
    },
    // Represents selecting specific columns from a table
    Projection {
        columns: Vec<String>,
    },
    // Represents a filter/selection operation
    Selection {
        condition: BExpr,
    },
    // Represents an expression evaluation
    ExpressionEvaluation {
        expression: BExpr,
    },
    //
    Grouping {
        expression: BExpr,
    },
    // Represents an aggregation operation
    Aggregation {
        function: AggregateFunction,
        expression: BExpr,
    },
    Having {
        typ: HavingExprType,
        expression: BExpr,
    },
    // Represents windowing operation
    Windowing {
        partition_criteria: Expr,
    },
    // Represents assigning an alias to the result of an expression
    AliasAssignment {
        alias: String,
        expression: Expr,
    },
}
