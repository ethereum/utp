use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Returns the UNIX timestamp truncated to a `u32`.
pub fn now_micros() -> u32 {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    now.as_micros() as u32
}

/// Returns the amount of time elapsed between `earlier_micros` and `later_micros`.
///
/// If `later_micros` is less than `earlier_micros`, then we assume that `later_micros`
/// has wrapped around the `u32` boundary.
pub fn duration_between(earlier_micros: u32, later_micros: u32) -> Duration {
    if later_micros < earlier_micros {
        Duration::from_micros((u32::MAX - earlier_micros + later_micros).into())
    } else {
        Duration::from_micros((later_micros - earlier_micros).into())
    }
}
