//! Models for TripAdvisor's XML Reviews feed structure.

use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Serialize, Deserialize)]
pub struct Property {
    pub id: u32,
    #[serde(rename = "Reviews")]
    pub reviews: Reviews,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Reviews {
    #[serde(rename = "Review")]
    pub inner: Vec<Review>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Review {
    pub id: u32,
    #[serde(rename = "DatePublished")]
    pub date: Option<String>,
    #[serde(rename = "ReviewURL")]
    pub url: Option<String>,
    #[serde(rename = "Language")]
    pub language: Option<String>,
    #[serde(rename = "Title")]
    pub title: Option<String>,
    #[serde(rename = "Text")]
    pub text: Option<String>,
    #[serde(rename = "TripType")]
    pub trip_type: Option<String>,
}
