//! Models for TripAdvisor's XML Reviews feed structure.

use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Serialize, Deserialize)]
pub struct Property {
    pub id: u32,
    #[serde(rename = "Reviews")]
    pub reviews: Vec<Review>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Review {
    pub id: u32,
    #[serde(rename = "DatePublished")]
    pub date: Option<String>,
    #[serde(rename = "ReviewURL")]
    pub url: Option<String>,
    #[serde(rename = "MoreReviewsURL")]
    pub more_reviews_url: Option<String>,
    #[serde(rename = "Rating")]
    pub rating: Option<f64>,
    #[serde(rename = "Language")]
    pub language: Option<String>,
    #[serde(rename = "Title")]
    pub title: Option<String>,
    #[serde(rename = "Text")]
    pub text: Option<String>,
    #[serde(rename = "TripType")]
    pub trip_type: Option<String>,
    #[serde(rename = "Author")]
    pub author: Option<Author>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Author {
    #[serde(rename = "AuthorName")]
    pub name: Option<String>,
}
