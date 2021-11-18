use crate::sources::tripadvisor::{models, Photos};

#[derive(Debug)]
pub enum BuildError {
    NotFound,
}

pub fn build_photo(property: models::photos::Property) -> Result<Photos, BuildError> {
    let id = property.id;

    let urls: Vec<_> = property
        .photos
        .inner
        .into_iter()
        .filter_map(|photo| {
            (photo.original_size_url)
                .or(photo.standard_size_url)
                .or(photo.full_size_url)
                .or(photo.large_thumbnail_url)
                .or(photo.thumbnail_url)
        })
        .collect();

    if urls.is_empty() {
        Err(BuildError::NotFound)
    } else {
        Ok(Photos { id, urls })
    }
}
