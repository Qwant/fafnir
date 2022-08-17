use super::models::Property;

#[derive(Debug, Eq, Hash, PartialEq)]
pub enum BuildError {
    NotFound,
}

pub fn build_reviews(property: Property) -> Result<(u32, Vec<String>), BuildError> {
    let reviews = property
        .reviews
        .iter()
        .map(|review| {
            serde_json::to_string(&review)
                .map_err(|_err| BuildError::NotFound)
                .ok()
                .unwrap()
        })
        .collect();

    Ok((property.id, reviews))
}
