use super::models::Property;
use crate::sources::tripadvisor::reviews::models::Reviews;

#[derive(Debug, Eq, Hash, PartialEq)]
pub enum BuildError {
    NotFound,
}

pub fn build_review(property: Property) -> Result<(u32, Reviews), BuildError> {
    Ok((property.id, property.reviews))
}
