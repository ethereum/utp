use std::time::Duration;

use crate::congestion;

#[derive(Clone, Copy, Debug)]
pub struct UtpConfig {
    pub max_packet_size: u16,
    pub max_conn_attempts: usize,
    pub max_idle_timeout: Duration,
    pub initial_timeout: Duration,
    pub min_timeout: Duration,
    pub max_timeout: Duration,
    pub target_delay: Duration,
}

impl Default for UtpConfig {
    fn default() -> Self {
        let max_idle_timeout = Duration::from_secs(10);
        Self {
            max_conn_attempts: 3,
            max_idle_timeout,
            max_packet_size: congestion::DEFAULT_MAX_PACKET_SIZE_BYTES as u16,
            initial_timeout: congestion::DEFAULT_INITIAL_TIMEOUT,
            min_timeout: congestion::DEFAULT_MIN_TIMEOUT,
            max_timeout: max_idle_timeout,
            target_delay: Duration::from_micros(congestion::DEFAULT_TARGET_MICROS.into()),
        }
    }
}

impl From<UtpConfig> for congestion::Config {
    fn from(value: UtpConfig) -> Self {
        Self {
            max_packet_size_bytes: u32::from(value.max_packet_size),
            initial_timeout: value.initial_timeout,
            min_timeout: value.min_timeout,
            max_timeout: value.max_timeout,
            target_delay_micros: value.target_delay.as_micros() as u32,
            ..Default::default()
        }
    }
}
