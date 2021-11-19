pub mod photos;
pub mod pois;

pub fn build_id(ta_id: u32) -> String {
    format!("ta:poi:{}", ta_id)
}
