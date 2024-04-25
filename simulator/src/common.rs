use serde::Serialize;

#[derive(Clone, Serialize)]
pub struct Start {}

#[derive(Clone, Serialize)]
pub struct ReportStatus {}

pub fn vector_intersection<T: Eq + Copy>(a: &Vec<T>, b: &Vec<T>) -> Vec<T> {
    a.iter().filter(|&x| b.contains(x)).copied().collect()
}
