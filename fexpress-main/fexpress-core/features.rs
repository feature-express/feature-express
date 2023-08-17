use crate::algo::topo_sort::topological_sort;
use crate::ast::core::Expr;
use crate::ast::traverse::traverse_expr;
use crate::map::HashMap;
use crate::sstring::SmallString;
use anyhow::{Context, Error, Result};
use itertools::Itertools;
use pest::Parser;
use std::convert::TryFrom;
use std::fmt::{Debug, Formatter};
use std::str::FromStr;

use crate::event::{AttributeKey, Event};
use crate::event_index::RawQuery;
use crate::parser::expr_parser::{generate_ast, ExprParser, Rule};
use crate::types::Timestamp;
use crate::value::Value;

pub trait FeatureExtractor {
    fn extract(&self, entity_id: &str, obs_date: &Timestamp, events: Vec<&Event>) -> Value;
}

impl Debug for dyn FeatureExtractor {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        write!(f, "FeatureExtractor")
    }
}

#[derive(Debug, Clone)]
pub struct Features {
    pub features: Vec<Feature>,
    // in what order to calculate them
    // it is an Option because the topological sort can return None
    // when the features have cycles. in this case the features
    // can be evaluted in any order and at some point it will fail
    // saying that some variable wasn't found
    pub calculation_order: Vec<usize>,
}

// Helper functions for extracting assigned and used variables
fn search_variable_assigns(expr: &Expr) -> Option<SmallString> {
    match expr {
        Expr::VariableAssign(v, _) => Some(v.clone()),
        _ => None,
    }
}

fn search_variable_usages(expr: &Expr) -> Option<SmallString> {
    match expr {
        Expr::ContextAttr(attribute_key) => match attribute_key {
            AttributeKey::Single(single) => Some(single.clone()),
            AttributeKey::Nested(_) => None,
        },
        _ => None,
    }
}

impl Features {
    pub fn new(features: Vec<Feature>) -> Self {
        let sorted_nodes = Self::sort_features(&features);
        let calculation_order = match &sorted_nodes {
            None => (0 as usize..features.len()).collect_vec(),
            Some(calculation_order) => calculation_order.clone(),
        };
        Self {
            features,
            calculation_order,
        }
    }

    fn sort_features(features: &Vec<Feature>) -> Option<Vec<usize>> {
        // prepare a graph of dependencies for features - look for variable assignments first
        // then look for usages in expressions
        let mut nodes: Vec<usize> = vec![];
        let mut variable_assigns_map: HashMap<usize, SmallString> = HashMap::new();
        let mut variable_assigns_rev_map: HashMap<SmallString, usize> = HashMap::new();
        let mut variable_usage_map: HashMap<usize, Vec<SmallString>> = HashMap::new();
        for (feature_index, feature) in features.iter().enumerate() {
            let variable_assigns: Vec<SmallString> =
                traverse_expr(&feature.expr, &search_variable_assigns);
            for assign in &variable_assigns {
                variable_assigns_map.insert(feature_index, assign.clone());
                variable_assigns_rev_map.insert(assign.clone(), feature_index);
            }
            let variable_usages: Vec<SmallString> =
                traverse_expr(&feature.expr, &search_variable_usages);
            for usage in &variable_usages {
                variable_usage_map
                    .entry(feature_index)
                    .or_default()
                    .push(usage.clone());
            }
            nodes.push(feature_index);
        }

        // construct edges - try to find used variables in variable assignments
        // if the variable cannot be found in the variable assignments then that's ok
        // it will be detected during the evaluation.
        let mut edges: Vec<(usize, usize)> = vec![];
        for (feature_index, used_variables) in &variable_usage_map {
            for variable in used_variables {
                if let Some(assignment_id) = variable_assigns_rev_map.get(variable) {
                    edges.push((*assignment_id, *feature_index))
                }
            }
        }

        let sorted_nodes = topological_sort(nodes, edges);
        sorted_nodes
    }
}

impl TryFrom<RawQuery> for Features {
    type Error = Error;

    fn try_from(value: RawQuery) -> Result<Self> {
        let features_vec = value.to_features_vec()?;
        Ok(Features::new(features_vec))
    }
}

#[derive(Clone, Debug)]
pub struct Feature {
    pub raw: String,
    pub expr: Expr,
    pub alias: Option<String>,
}

impl Feature {
    pub fn get_name(&self) -> String {
        if let Some(alias) = self.alias.clone() {
            alias
        } else {
            self.raw.clone()
        }
    }
}

impl AsRef<Expr> for Feature {
    fn as_ref(&self) -> &Expr {
        &self.expr
    }
}

impl Into<Expr> for Feature {
    fn into(self) -> Expr {
        self.expr
    }
}

impl FromStr for Feature {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let successful_parse = ExprParser::parse(Rule::single_expression, s);
        let pairs = successful_parse.context("parsing error")?;
        let expr = generate_ast(pairs);
        let (alias, expr) = match expr {
            Expr::Alias(alias, expr) => (Some(alias.to_string()), expr),
            _ => (None, Box::new(expr)),
        };
        Ok(Feature {
            raw: s.into(),
            expr: *expr,
            alias,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_creating_features_from_variable_assignments() {
        let query = RawQuery::SelectExpr(
            r#"
        SELECT
            @a + @b + @c as abc,
            @a := 1,
            @b := 2,
            @c := 3
        FOR
            @entities := user
        "#
            .into(),
        );
        let features_vec = query.to_features_vec().unwrap();
        let sorted_features: Option<Vec<usize>> = Features::sort_features(&features_vec);
        assert_eq!(sorted_features, Some(vec![1, 2, 3, 0]));
    }

    #[test]
    fn test_cyclical_dependencies() {
        let query = RawQuery::SelectExpr(
            r#"
        SELECT
            @a := @b + 1,
            @b := @a + 1
        FOR
            @entities := user
        "#
            .into(),
        );
        let features_vec = query.to_features_vec().unwrap();
        let sorted_features = Features::sort_features(&features_vec);
        assert_eq!(sorted_features, None, "Should detect a cycle");
    }

    #[test]
    fn test_nested_dependencies() {
        let query = RawQuery::SelectExpr(
            r#"
        SELECT
            @a := 1,
            @b := @a + 1,
            @c := @b + 1
        FOR
            @entities := user
        "#
            .into(),
        );
        let features_vec = query.to_features_vec().unwrap();
        println!("features {:?}", features_vec);
        let sorted_features = Features::sort_features(&features_vec).unwrap();
        assert_eq!(
            sorted_features,
            vec![0, 1, 2],
            "Should be sorted in nested order"
        );
    }

    #[test]
    fn test_no_dependencies() {
        let query = RawQuery::SelectExpr(
            r#"
        SELECT
            @a := 1,
            @b := 2,
            @c := 3
        FOR
            @entities := ser
        "#
            .into(),
        );
        let features_vec = query.to_features_vec().unwrap();
        let sorted_features = Features::sort_features(&features_vec).unwrap();
        assert!(
            sorted_features.iter().eq(vec![0, 1, 2].iter())
                || sorted_features.iter().eq(vec![2, 1, 0].iter()),
            "Order doesn't matter"
        );
    }

    #[test]
    fn test_expressions_without_variables() {
        let query = RawQuery::SelectExpr(
            r#"
        SELECT
            last(event_time) OVER past,
            first(event_time) OVER future,
            last(event_time) OVER past WHERE event_type = 'PurchaseEvent',
            first(event_time) OVER future WHERE event_type = 'PurchaseEvent',
            COUNT(*) OVER past WHERE event_type = 'PurchaseEvent',
            COUNT(*) OVER future WHERE event_type = 'PurchaseEvent',
            sum(purchaseAmount) over future where event_type = 'PurchaseEvent' as target,
            sum(1) over last 3 days where event_type = 'PurchaseEvent',
            last(event_time) over last 3 days where event_type = 'ViewEvent' group by viewedItem,
            sum(purchaseAmount) over last 3 days where event_type = 'PurchaseEvent',
            avg(purchaseAmount) over last 3 days where event_type = 'PurchaseEvent'
        FOR
            @entities := user
        "#
            .into(),
        );
        let features_vec = query.to_features_vec().unwrap();
        let sorted_features = Features::sort_features(&features_vec).unwrap();

        // Since these expressions don't have dependencies on other expressions,
        // their order should remain unchanged.
        let expected_order: Vec<usize> = (0..features_vec.len()).collect();
        assert_eq!(
            sorted_features, expected_order,
            "Order should remain unchanged"
        );
    }
    // Helper function for checking if the dependencies are satisfied
    fn check_dependencies_satisfied(sorted_indices: Vec<usize>, features: &Vec<Feature>) {
        // Map to keep track of whether a variable has been assigned before it is used.
        let mut assigned_vars = HashMap::new();

        // Iterate over the sorted features
        for &index in &sorted_indices {
            let feature = &features[index];

            // Collect variables assigned by this feature
            let assigned_vars_in_feature = traverse_expr(&feature.expr, &search_variable_assigns);
            for var in assigned_vars_in_feature {
                assigned_vars.insert(var, true);
            }

            // Collect variables used by this feature
            let used_vars_in_feature = traverse_expr(&feature.expr, &search_variable_usages);
            for var in used_vars_in_feature {
                // Assert that the variable was assigned before it was used
                assert_eq!(
                    assigned_vars.get(&var),
                    Some(&true),
                    "Variable used before assignment"
                );
            }
        }
    }

    // Test cases

    #[test]
    fn test_mixed_expressions_simple() {
        let query = RawQuery::SelectExpr(
            r#"
        SELECT
            @a := 1,
            sum(purchaseAmount) over future where event_type = 'PurchaseEvent' as target,
            @b := @a + 1,
            @c := @b + 1,
            @c + @a as sum_c_a,
            COUNT(*) OVER past WHERE event_type = 'PurchaseEvent'
        FOR
            @entities := user
        "#
            .into(),
        );
        let features_vec = query.to_features_vec().unwrap();
        let sorted_indices = Features::sort_features(&features_vec).unwrap();
        check_dependencies_satisfied(sorted_indices, &features_vec);
    }

    #[test]
    fn test_mixed_expressions_complex() {
        let query = RawQuery::SelectExpr(
            r#"
        SELECT
            @a := 1,
            last(event_time) OVER past,
            @b := @a + 1,
            first(event_time) OVER future WHERE event_type = 'PurchaseEvent',
            @c := @b + 1,
            COUNT(*) OVER future WHERE event_type = 'PurchaseEvent',
            @c + @a as sum_c_a,
            sum(purchaseAmount) over last 3 days where event_type = 'PurchaseEvent'
        FOR
            @entities := user
        "#
            .into(),
        );
        let features_vec = query.to_features_vec().unwrap();
        let sorted_indices = Features::sort_features(&features_vec).unwrap();
        check_dependencies_satisfied(sorted_indices, &features_vec);
    }
}
