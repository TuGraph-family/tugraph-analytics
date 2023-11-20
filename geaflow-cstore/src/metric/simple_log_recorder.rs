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

use std::sync::Arc;

use metrics::{
    Counter, CounterFn, Gauge, GaugeFn, Histogram, HistogramFn, Key, KeyName, Recorder,
    SharedString, Unit,
};

use crate::{config::MetricConfig, log_util::info, metric::metric_recorder::MetricRecorder};

#[derive(Default)]
pub struct SimpleLogRecorder;

impl MetricRecorder for SimpleLogRecorder {
    fn install(_config: &MetricConfig) {
        let recorder = SimpleLogRecorder;
        metrics::set_boxed_recorder(Box::new(recorder)).unwrap()
    }
}

impl Recorder for SimpleLogRecorder {
    fn describe_counter(&self, key_name: KeyName, unit: Option<Unit>, description: SharedString) {
        info!(
            "(counter) registered key {} with unit {:?} and description {:?}",
            key_name.as_str(),
            unit,
            description
        );
    }

    fn describe_gauge(&self, key_name: KeyName, unit: Option<Unit>, description: SharedString) {
        info!(
            "(gauge) registered key {} with unit {:?} and description {:?}",
            key_name.as_str(),
            unit,
            description
        );
    }

    fn describe_histogram(&self, key_name: KeyName, unit: Option<Unit>, description: SharedString) {
        info!(
            "(histogram) registered key {} with unit {:?} and description {:?}",
            key_name.as_str(),
            unit,
            description
        );
    }

    fn register_counter(&self, key: &Key) -> Counter {
        Counter::from_arc(Arc::new(PrintHandle(key.clone())))
    }

    fn register_gauge(&self, key: &Key) -> Gauge {
        Gauge::from_arc(Arc::new(PrintHandle(key.clone())))
    }

    fn register_histogram(&self, key: &Key) -> Histogram {
        Histogram::from_arc(Arc::new(PrintHandle(key.clone())))
    }
}

struct PrintHandle(Key);

impl CounterFn for PrintHandle {
    fn increment(&self, value: u64) {
        info!("counter increment for '{}': {}", self.0, value);
    }

    fn absolute(&self, value: u64) {
        info!("counter absolute for '{}': {}", self.0, value);
    }
}

impl GaugeFn for PrintHandle {
    fn increment(&self, value: f64) {
        info!("gauge increment for '{}': {}", self.0, value);
    }

    fn decrement(&self, value: f64) {
        info!("gauge decrement for '{}': {}", self.0, value);
    }

    fn set(&self, value: f64) {
        info!("gauge set for '{}': {}", self.0, value);
    }
}

impl HistogramFn for PrintHandle {
    fn record(&self, value: f64) {
        info!("histogram record for '{}': {}", self.0, value);
    }
}
