use std::fs::OpenOptions;
use std::io::Write;

use chrono::{Local, NaiveDateTime};
use env_logger::{Target, WriteStyle};
use env_logger::fmt::Formatter;
use log::{LevelFilter, Record};

#[macro_export]
macro_rules! clog {
    ($($arg:tt)*) => {{
        log::info!(concat!("\x1b[38;2;255;165;0m", "{}\x1b[0m"), format_args!($($arg)*));
    }};
}

#[macro_export]
macro_rules! slog {
    ($($arg:tt)*) => {{
        log::info!(concat!("\x1b[38;2;100;100;100m", "{}\x1b[0m"), format_args!($($arg)*));
    }};
}

pub fn setup(to_file: bool) {
    let file = if to_file {
        let now: NaiveDateTime = Local::now().naive_local();
        let file_name = format!("{}.log", now.format("%Y-%m-%d"));

        Some(OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_name)
            .unwrap())
    } else {
        None
    };

    let formatter = |buf: &mut Formatter, record: &Record| {
        let format = Local::now().format("%Y-%m-%d %H:%M:%S");

        writeln!(buf, "{} [{}] {}", format, record.level(), record.args())
    };

    env_logger::Builder::new()
        .format(formatter)
        .filter(None, LevelFilter::Info)
        .write_style(WriteStyle::Always)
        .target(
            if to_file {
                Target::Pipe(Box::new(file.unwrap()))
            } else {
                Target::Stdout
            }
        )
        .init();
}
