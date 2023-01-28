use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Debug)]
pub struct Timestamp(SystemTime);

impl Timestamp {
    pub fn now() -> Self {
        Self(SystemTime::now())
    }
}

/// Returns the UNIX timestamp truncated to a `u32`.
pub fn now_micros() -> u32 {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    now.as_micros() as u32
}

/// Returns the number of microseconds that have elapsed between `earlier_micros` and
/// `later_micros`.
///
/// Returns `None` if `earlier_micros` is later than `later_micros`.
pub fn duration_between(earlier_micros: u32, later_micros: u32) -> Option<u32> {
    if later_micros < earlier_micros {
        None
    } else {
        Some(later_micros - earlier_micros)
    }
}
