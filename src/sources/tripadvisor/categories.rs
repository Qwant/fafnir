/// Build a corresponding OSM class / subclass for a given pair of
/// category / sub_category from TripAdvisor.
pub fn get_class_subclass(
    category: &str,
    sub_category: Option<&str>,
) -> Option<(&'static str, &'static str)> {
    Some(match (category, sub_category) {
        ("Hotel", _) => ("hotel", "hotel"),
        ("Restaurant", _) => ("restaurant", "restaurant"),
        _ => return None,
    })
}
