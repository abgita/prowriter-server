use std::env;
use std::error::Error;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

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

pub fn get_truncated_uuid() -> String {
    let uuid = Uuid::new_v4();

    let truncated_uuid = uuid.as_bytes()[..8].to_vec();

    hex::encode(truncated_uuid)
}

pub fn get_new_user_pid() -> String {
    get_truncated_uuid()
}

pub fn get_short_pid() -> String {
    let uuid = Uuid::new_v4();
    let truncated_uuid = uuid.as_bytes()[..3].to_vec();
    
    hex::encode(truncated_uuid)
}

pub fn get_new_uuid() -> String {
    Uuid::new_v4().to_string()
}

pub fn create_dirs_if_not_exists(directory: &std::path::Path) -> Result<(), Box<dyn Error>> {
    if !directory.exists() {
        std::fs::create_dir_all(directory)?;
    }

    Ok(())
}
