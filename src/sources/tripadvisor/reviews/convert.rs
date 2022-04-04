use super::models::Property;

#[derive(Debug, Eq, Hash, PartialEq)]
pub enum BuildError {
    NotFound,
}

pub fn build_review(property: Property) -> Result<(u32, String), BuildError> {
    let reviews_json_string = match serde_json::to_string(&property.reviews) {
        Ok(string) => string,
        Err(_e) => return Err(BuildError::NotFound),
    };
    Ok((property.id, reviews_json_string))
}
