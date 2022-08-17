use super::models::Property;
use tracing::warn;

#[derive(Debug, Eq, Hash, PartialEq)]
pub enum BuildError {
    NotFound,
}

pub fn build_photo(property: Property) -> Result<(u32, String), BuildError> {
    let mut all_urls = property.photos.into_iter().filter_map(|photo| {
        (photo.original_size.url)
            .or(photo.standard_size.url)
            .or(photo.full_size.url)
            .or(photo.large_thumbnail.url)
            .or(photo.thumbnail.url)
    });

    let photo_url = all_urls.next().ok_or(BuildError::NotFound)?;

    if all_urls.next().is_some() {
        // There is nothing that would prevents TripAdvisor to provide several
        // images to us someday.
        warn!("found several URLs for a TripAdvisor property: only one will be included");
    }

    Ok((property.id, photo_url))
}
