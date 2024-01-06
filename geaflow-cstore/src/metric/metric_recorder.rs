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

#![allow(non_upper_case_globals)]

use std::sync::Once;

use strum_macros::{Display, EnumString};

use crate::{
    config::MetricConfig,
    metric::{prometheus_recorder::PrometheusRecorder, simple_log_recorder::SimpleLogRecorder},
};

#[derive(Debug, Clone, Copy, EnumString, Display, PartialEq)]
#[strum(ascii_case_insensitive)]
pub enum ReporterType {
    Log = 1,
    Prometheus,
}

pub trait MetricRecorder {
    fn install(config: &MetricConfig);
}

static INIT: Once = Once::new();

pub fn try_init(config: &MetricConfig) {
    INIT.call_once(|| {
        let reporter_type: ReporterType = config.reporter_type;
        if let ReporterType::Log = reporter_type {
            SimpleLogRecorder::install(config);
        } else {
            PrometheusRecorder::install(config);
        }
    });
}

#[cfg(test)]
mod tests {
    use std::{thread, time};

    use metrics::{
        absolute_counter, counter, gauge, histogram, increment_counter, increment_gauge,
        register_counter, register_gauge, register_histogram,
    };

    use crate::{
        log_util,
        log_util::{LogLevel, LogType},
        metric::metric_recorder,
        CStoreConfig,
    };

    #[test]
    fn test_metric_recorder() {
        let config = CStoreConfig::default();

        log_util::try_init(LogType::ConsoleAndFile, LogLevel::Debug, 0);
        metric_recorder::try_init(&config.metric);

        counter!("test", (|| 3)());

        // And registering them:
        let counter1 = register_counter!("test_counter");
        counter1.increment(1);

        let gauge1 = register_gauge!("test_gauge");
        gauge1.increment(1.0);

        let histogram1 = register_histogram!("test_histogram");
        histogram1.record(0.57721);

        // All the supported permutations of `counter!` and its increment/absolute
        // versions:
        counter!("bytes_sent", 64);
        absolute_counter!("bytes_sent", 64);
        increment_counter!("requests_processed");

        // All the supported permutations of `gauge!` and its increment/decrement
        // versions:qq
        gauge!("connection_count", 300.0);
        increment_gauge!("connection_count", 300.0);

        // All the supported permutations of `histogram!`
        histogram!("svc.execution_time", 70.0);

        // thread::sleep(time::Duration::from_secs(600));
    }

    #[test]
    fn test_multi_thread_metric_recorder() {
        register_gauge!("counter");
        let fun = move || {
            let config = CStoreConfig::default();
            metric_recorder::try_init(&config.metric);
            gauge!("counter", 1f64);
        };
        let handle_1 = thread::spawn(fun);
        let handle_2 = thread::spawn(fun);

        handle_1.join().unwrap();
        handle_2.join().unwrap();
        thread::sleep(time::Duration::from_secs(1));
    }
}
