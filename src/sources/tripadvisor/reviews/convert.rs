use super::models::Property;

#[derive(Debug, Eq, Hash, PartialEq)]
pub enum BuildError {
    NotFound,
}

pub fn build_review(property: Property) -> Result<(u32, String), BuildError> {
    Ok((property.id, "test".to_string()))
}
