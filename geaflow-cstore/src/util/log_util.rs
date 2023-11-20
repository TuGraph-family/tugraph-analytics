// Copyright 2023 AntGroup CO., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

use std::{
    path::PathBuf,
    sync::{Once, OnceLock},
};

use strum_macros::{Display, EnumString};
use time::UtcOffset;
pub use tracing::{debug, info, trace};
use tracing_appender::{non_blocking, non_blocking::WorkerGuard, rolling};
use tracing_error::ErrorLayer;
use tracing_subscriber::{
    filter::EnvFilter, fmt, fmt::format, layer::SubscriberExt, util::SubscriberInitExt, Registry,
};
use uuid::Uuid;

use crate::{
    file::file_operator::{FileOperator, LocalFileOperator},
    CStoreConfig,
};

#[macro_export]
macro_rules! error_and_panic {
    ($($arg:tt)*) => {
        {
            let error_message = format!($($arg)*);
            tracing::error!("{}", error_message);
            panic!("{}", error_message);
        }
    };
}

#[macro_export]
macro_rules! s_info {
    ($sid:expr, $($arg:tt)*) => {
        {
            let info = format!($($arg)*);
            tracing::info!("SID[{}] {}", $sid, info);
        }
    };
}

#[macro_export]
macro_rules! s_debug {
    ($sid:expr, $($arg:tt)*) => {
        {
            let info = format!($($arg)*);
            tracing::debug!("SID[{}] {}", $sid, info);
        }
    };
}

#[macro_export]
macro_rules! s_trace {
    ($sid:expr, $($arg:tt)*) => {
        {
            let info = format!($($arg)*);
            tracing::trace!("SID[{}] {}", $sid, info);
        }
    };
}

static INIT: Once = Once::new();
static GUARD: OnceLock<WorkerGuard> = OnceLock::new();

#[derive(Clone, Copy, Display, EnumString, PartialEq, Debug, Default, Eq)]
#[strum(ascii_case_insensitive)]
pub enum LogType {
    #[default]
    ConsoleAndFile = 0,
    OnlyConsole,
    OnlyFile,
}

#[derive(Clone, Copy, Display, EnumString, PartialEq, Debug, Default, Eq)]
#[strum(ascii_case_insensitive)]
pub enum LogLevel {
    #[default]
    DependOnEnv = 0,
    Info,
    Debug,
    Trace,
}

/// Setup function that is only run once, even if called multiple times.
pub fn try_init(log_type: LogType, log_level: LogLevel, shard_index: u32) {
    INIT.call_once(|| {
        let env_filter = match log_level {
            LogLevel::DependOnEnv => {
                EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
            }
            LogLevel::Info => EnvFilter::new("info"),
            LogLevel::Debug => EnvFilter::new("debug"),
            LogLevel::Trace => EnvFilter::new("trace"),
        };
        let timer = time::format_description::parse(
            "[year]-[month padding:zero]-[day padding:zero] [hour]:[minute]:[second]",
        )
        .unwrap();

        let time_offset: UtcOffset = UtcOffset::from_hms(8, 0, 0).unwrap();
        let timer = fmt::time::OffsetTime::new(time_offset, timer);

        let fmt = format()
            .with_target(false)
            .with_file(true)
            .with_line_number(true)
            .with_thread_ids(true)
            .with_thread_names(false)
            .with_timer(timer);

        let log_name = format!("cstore_{}.log", shard_index);

        let log_path = PathBuf::new().join("log").join(&log_name);
        LocalFileOperator.remove_file(&log_path).unwrap();

        let file_appender = rolling::never("log", log_name);

        let (non_blocking_appender, guard) = non_blocking(file_appender);

        match log_type {
            LogType::OnlyFile => {
                let file_layer = fmt::layer()
                    .event_format(fmt)
                    .with_ansi(false)
                    .with_writer(non_blocking_appender);
                Registry::default()
                    .with(env_filter)
                    .with(ErrorLayer::default())
                    .with(file_layer)
                    .init();
            }

            LogType::OnlyConsole => {
                let formatting_layer = fmt::layer().event_format(fmt).with_writer(std::io::stdout);
                Registry::default()
                    .with(env_filter)
                    .with(ErrorLayer::default())
                    .with(formatting_layer)
                    .init();
            }

            LogType::ConsoleAndFile => {
                let formatting_layer = fmt::layer()
                    .event_format(fmt.clone())
                    .with_writer(std::io::stdout);
                let file_layer = fmt::layer()
                    .event_format(fmt)
                    .with_ansi(false)
                    .with_writer(non_blocking_appender);
                Registry::default()
                    .with(env_filter)
                    .with(ErrorLayer::default())
                    .with(file_layer)
                    .with(formatting_layer)
                    .init();
            }
        };

        color_eyre::install().unwrap();
        GUARD.set(guard).expect("Failed to set tracing guard.");
    });
}

pub fn generate_trace_id(config: &CStoreConfig) -> String {
    if config.log.enable_trace {
        Uuid::new_v4().to_string()
    } else {
        "".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;

    #[test]
    fn test_error_and_panic() {
        super::try_init(LogType::ConsoleAndFile, LogLevel::Debug, 0);

        let result = std::panic::catch_unwind(|| {
            error_and_panic!(
                "This is an error message: {:?}",
                Error::StdFileError("test/0.vs".to_string(), "file not found".to_string())
            );
        });

        assert!(result.is_err(), "error was expected");
    }
}
