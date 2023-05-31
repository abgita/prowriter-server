use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn get_env_or_default<T: std::str::FromStr>(name: &str, default: T) -> T {
    match env::var(name) {
        Ok(val) => val.parse::<T>().unwrap_or(default),
        Err(_) => default,
    }
}

pub fn get_env_var(var_name: &str) -> Option<String> {
    match env::var(var_name) {
        Ok(val) => Some(val),
        Err(_) => None,
    }
}

pub fn current_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs()
}

pub fn current_timestamp_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis()
}
