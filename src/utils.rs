use serde::Serializer;
use std::fmt::Debug;

/// Custom serializer that uses Debug formatting
pub fn serialize_as_debug<T, S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
where
    T: Debug,
    S: Serializer,
{
    serializer.serialize_str(&format!("{:#?}", value))
}
