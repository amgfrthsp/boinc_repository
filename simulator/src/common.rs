use std::cmp::Ordering;

use serde::Serialize;

#[derive(Clone, Serialize)]
pub struct Finish {}

#[derive(Clone, Serialize)]
pub struct ReportStatus {}

// Float wrapper for ordered structs
#[derive(Debug)]
pub struct FloatWrapper(pub f64);

impl PartialEq for FloatWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for FloatWrapper {}

impl PartialOrd for FloatWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FloatWrapper {
    fn cmp(&self, other: &Self) -> Ordering {
        // Handle NaN as the largest value
        if self.0.is_nan() && other.0.is_nan() {
            Ordering::Equal
        } else if self.0.is_nan() {
            Ordering::Greater
        } else if other.0.is_nan() {
            Ordering::Less
        } else {
            self.0.partial_cmp(&other.0).unwrap_or(Ordering::Equal)
        }
    }
}
