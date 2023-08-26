use crate::ast::core::Expr;
use crate::ast::traverse::ExprVisitor;
use crate::event::AttributeName;

use crate::event_store::EventStore;
use crate::features::Features;
use crate::map::HashMap;
use crate::map::HashSet;
use crate::sstring::SmallString;
use crate::types::Entities;
use crate::value::ValueType;
use anyhow::Result;
use itertools::Itertools;

pub struct UntypedAttributeRewriteVisitor {
    pub attribute_types: HashMap<AttributeName, HashSet<ValueType>>,
}

impl ExprVisitor for UntypedAttributeRewriteVisitor {
    fn visit(&mut self, expr: &mut Expr) {
        if let Expr::AttrUntyped(key) = expr {
            let attribute_name = AttributeName(key.to_kstring());
            if let Some(attribute_values) = self.attribute_types.get(&attribute_name) {
                if attribute_values.len() == 1 {
                    let attribute_value = attribute_values.iter().collect_vec();
                    if let Some(value_type) = attribute_value.first() {
                        *expr = match value_type {
                            ValueType::Bool => Expr::AttrBool(key.clone()),
                            ValueType::Num => Expr::AttrNum(key.clone()),
                            ValueType::Int => Expr::AttrInt(key.clone()),
                            ValueType::MapNum => Expr::AttrMapNum(key.clone()),
                            ValueType::MapStr => Expr::AttrMapStr(key.clone()),
                            ValueType::Str => Expr::AttrMapStr(key.clone()),
                            ValueType::Date => Expr::AttrDate(key.clone()),
                            ValueType::DateTime => Expr::AttrDateTime(key.clone()),
                            ValueType::VecCat => Expr::AttrVecStr(key.clone()),
                            ValueType::VecNum => Expr::AttrVecNum(key.clone()),
                            ValueType::VecInt => Expr::AttrVecInt(key.clone()),
                            ValueType::VecBool => Expr::AttrVecBool(key.clone()),
                            ValueType::Map => expr.clone(),
                            ValueType::None => expr.clone(),
                            ValueType::Wildcard => expr.clone(),
                            ValueType::NotCalculatedYet => expr.clone(),
                        }
                    }
                }
            }
        }
    }
}

/// rewrites untyped attributes to
pub fn rewrite_untyped_attributes(
    features: &mut Features,
    event_store: &mut dyn EventStore,
) -> Result<()> {
    let attribute_value_types = event_store.get_attribute_value_types();
    let mut visitor = UntypedAttributeRewriteVisitor {
        attribute_types: attribute_value_types,
    };
    for feature in features.features.iter_mut() {
        feature.expr.visit(&mut visitor);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::event_index::EventContext;
    use crate::event_index::EventScopeConfig;
    use crate::event_index::QueryConfig;
    use crate::event_index::RawQuery;
    use crate::features_rewrite::Features;
    use crate::obs_dates::ObservationDatesConfig;
    use crate::tests::fake_nba::generate_nba_game_events;
    use regex::Regex;
    use std::convert::TryFrom;

    #[test]
    fn test_rewrite() {
        let event_store_settings = QueryConfig {
            parallel: true,
            include_events_on_obs_date: true,
        };
        let entity_query = EventScopeConfig::AllEvents;
        let mut event_context = EventContext::new_memory();
        let events = generate_nba_game_events(100);
        for event in &events {
            event_context.new_event(event.clone());
        }

        let obs_dates = ObservationDatesConfig::AllEvents;
        let query = r#"
        SELECT
            obs_dt as obs_dt,
            AVG(game_result = "away") OVER past as win_perc_away,
        FOR
            @entities := home
        "#
        .trim()
        .replace(" past ", " last 30 day ")
        .to_string();

        let comment_regex = Regex::new(r"//.*").unwrap();
        let query = comment_regex.replace_all(&query, "").to_string();

        let raw_query = RawQuery::SelectExpr(query.into());

        let query_config = QueryConfig {
            include_events_on_obs_date: true,
            parallel: false,
        };

        let mut features = Features::try_from(raw_query).unwrap();

        println!("{:#?}", features);
        rewrite_untyped_attributes(&mut features, &mut event_context.event_store);
        println!("{:#?}", features);
    }
}
