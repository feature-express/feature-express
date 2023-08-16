use crate::ast::core::{AggrExpr, Expr};
use crate::ast::simple_graph::WeightedDirectedGraph;

use crate::map::HashSet;

use itertools::Itertools;
use std::fmt::{Display, Formatter};
use std::iter::FromIterator;

/*
This function analyzes the expressions:
1. Searches for aggregations
2. Extracts unique expressions that are formed from
    - projections - for example in `avg(pressure+1) over past` `pressure + 1` would be the projection
    - where clauses
    - group by clauses
    - having clauses
*/
pub fn extract_base_expressions(expr: &Expr) -> Vec<Expr> {
    let all_expressions = expr.extract_aggregations();
    let agg_expressions: Vec<&AggrExpr> = all_expressions
        .iter()
        .map(|v| v.as_aggr().unwrap())
        .collect_vec();
    let mut all_projections = HashSet::new();
    agg_expressions.into_iter().for_each(|agg| {
        all_projections.insert(*agg.agg_expr.clone());
        if let Some(where_expr) = agg.cond.clone() {
            all_projections.insert(*where_expr.clone());
        }
        if let Some(groupby_expr) = agg.groupby.clone() {
            all_projections.insert(*groupby_expr.clone());
        }
        if let Some(having_expr) = agg.having.clone() {
            all_projections.insert(*having_expr.expr.clone());
        }
    });
    Vec::from_iter(all_projections.into_iter())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CalculationNode {
    AggregationAlias(Expr),
    Aggregation(Expr),
    Expression(Expr),
    Attribute(Expr),
}

impl CalculationNode {
    pub fn weight(&self) -> usize {
        match self {
            CalculationNode::AggregationAlias(_) => 4,
            CalculationNode::Aggregation(_) => 3,
            CalculationNode::Expression(_) => 2,
            CalculationNode::Attribute(_) => 1,
        }
    }
}

impl Display for CalculationNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CalculationNode::AggregationAlias(expr) => write!(f, "Aggregation Alias ({})", expr),
            CalculationNode::Aggregation(expr) => write!(f, "Aggregation ({})", expr),
            CalculationNode::Expression(expr) => write!(f, "Expression ({})", expr),
            CalculationNode::Attribute(expr) => write!(f, "Attribute ({})", expr),
        }
    }
}

pub fn sort_sub_expressions(ast: Expr) -> Option<Vec<(CalculationNode, Vec<CalculationNode>)>> {
    if let Expr::Select(select) = ast {
        let mut graph = WeightedDirectedGraph::<CalculationNode>::new();
        for expr in select.expressions {
            for (node1, node2) in expr.extract_calculation_node_edges() {
                graph.add_edge(node2.clone(), node2.weight(), node1.clone(), node1.weight());
            }
        }
        let sorted_node_indices = graph
            .augmented_topo_sort_weighted(true)
            .expect("Cannot sort the calculation");
        Some(sorted_node_indices)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::core::{AggrExpr, Expr};
    use crate::ast::simple_graph::WeightedDirectedGraph;
    use crate::ast::traverse::traverse_expr;
    use crate::parser::expr_parser::{generate_ast, ExprParser, Rule};
    use itertools::Itertools;
    use pest::Parser;

    #[test]
    fn test_full_query_parse() {
        let successful_parse = ExprParser::parse(
            Rule::full_query,
            r#"
            select
                avg(pressure) over past as a,
                sum(pressure + 1) over past as b1,
                sum(pressure + 1) over past as b2,
                sum(pressure + 1) over past as b3,
                sum(pressure + 1) over past as b4,
                sum(pressure + 5) over past / count(*) over past as e,
                max(pressure + 2) over past where pressure > 0 as c,
                max(pressure + 2) over past where pressure > 0 having max temperature as d,
            for
                @entities := user"#,
        );
        let ast = match successful_parse {
            Ok(parsed) => generate_ast(parsed),
            Err(e) => panic!(e.to_string()),
        };
        let expressions = extract_base_expressions(&ast);
        for expr in &expressions {
            println!("{}", expr);
        }
        assert_eq!(expressions.len(), 7);
    }

    #[test]
    fn test_dag() {
        let successful_parse = ExprParser::parse(
            Rule::full_query,
            r#"
            select
                avg(pressure) over past as a,
                sum(pressure + 1) over past as b1,
                sum(pressure + 2) over past as b2,
            for
                @entities := user"#,
        );
        let ast = match successful_parse {
            Ok(parsed) => generate_ast(parsed),
            Err(e) => panic!(e.to_string()),
        };
        let subexpressions = sort_sub_expressions(ast.clone()).unwrap();
        for expr in subexpressions {
            println!("{:?}", expr);
        }

        println!("\n\n");
    }
}
