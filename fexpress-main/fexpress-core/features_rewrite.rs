use crate::ast::core::Expr;
use crate::ast::traverse::ExprVisitor;
use crate::event::AttributeName;
use crate::event_index::QueryConfig;
use crate::event_store::EventStore;
use crate::features::Features;
use crate::map::HashMap;
use crate::map::HashSet;
use crate::sstring::SmallString;
use crate::types::Entities;
use crate::value::ValueType;
use anyhow::Result;

pub struct UntypedAttributeRewriteVisitor {
    pub attribute_types: HashMap<AttributeName, HashSet<ValueType>>,
}

impl ExprVisitor for UntypedAttributeRewriteVisitor {
    fn visit(&mut self, expr: &mut Expr) {
        todo!()
    }
}

/// rewrites untyped attributes to
pub fn rewrite_untyped_attributes(
    features: &mut Features,
    event_store: &mut EventStore,
) -> Result<()> {
    // let attribute_name = a!(attribute.to_kstring());
    // let event_index = context.event_index.ok_or(anyhow!("event index needed"))?;
    // let value_types: Vec<ValueType> = event_index
    //     .event_store
    //     .get_attribute_value_type(&attribute_name)
    //     .with_context(|| {
    //         format!(
    //             "Cannot find attribute name {:} in the schema - available attributes are {:?}",
    //             attribute,
    //             event_index.event_store.get_schema()
    //         )
    //     })?
    //     .into_iter()
    //     .collect();
    Ok(())
}
