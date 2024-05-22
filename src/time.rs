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

#[test]
fn test_now_duration() {
    let micros_a = now_micros();
    let micros_b = now_micros();

    assert!(micros_a < micros_b);

    let duration = duration_between(micros_a, micros_b);

    let duration_micros = duration.as_micros();
    assert!(duration_micros > 0);
    assert!(duration_micros < 2000);
}

#[test]
fn test_wrapped_duration() {
    // Max u32 value minus 10.
    let earlier_micros: u32 = 4294967285;
    // Wrapped around by 10.
    let later_micros: u32 = 10;
    // earlier_micros > later_micros
    let duration = duration_between(earlier_micros, later_micros);
    // With 10 microseconds on each side of the max, total should be 20 microseconds.
    assert_eq!(duration.as_micros(), 20);
}
