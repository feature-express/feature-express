use schemars::gen::SchemaGenerator;
use schemars::schema::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::ops::Deref;
use vec1::Vec1;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Vec1Wrapper<T>(pub Vec1<T>);

impl<T> Deref for Vec1Wrapper<T> {
    type Target = Vec1<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> From<Vec1<T>> for Vec1Wrapper<T> {
    fn from(vec: Vec1<T>) -> Self {
        Vec1Wrapper(vec)
    }
}

impl<T> JsonSchema for Vec1Wrapper<T>
where
    T: JsonSchema,
{
    fn is_referenceable() -> bool {
        false
    }

    fn schema_name() -> String {
        format!("Array_of_{}", T::schema_name())
    }

    fn json_schema(gen: &mut SchemaGenerator) -> Schema {
        SchemaObject {
            instance_type: Some(InstanceType::Array.into()),
            array: Some(Box::new(ArrayValidation {
                items: Some(gen.subschema_for::<T>().into()),
                ..Default::default()
            })),
            ..Default::default()
        }
        .into()
    }
}
