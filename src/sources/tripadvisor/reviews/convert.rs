use super::models::{Property, Review};

#[derive(Debug, Eq, Hash, PartialEq)]
pub enum BuildError {
    NotFound,
}

pub fn build_review(property: Property) -> Result<(u32, Vec<Review>), BuildError> {
    Ok((property.id, property.reviews.inner))
}
