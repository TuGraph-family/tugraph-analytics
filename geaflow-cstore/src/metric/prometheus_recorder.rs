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

use std::{process, time::Duration};

use hostname::get_hostname;
use metrics_exporter_prometheus::PrometheusBuilder;
use metrics_util::MetricKindMask;

use crate::{config::MetricConfig, metric::metric_recorder::MetricRecorder};

const JOB_NAME_TAG: &str = "jobName";
const INSTANCE_TAG: &str = "instance";

pub struct PrometheusRecorder;

impl MetricRecorder for PrometheusRecorder {
    fn install(config: &MetricConfig) {
        let mut builder: PrometheusBuilder = PrometheusBuilder::new();
        let instance = format!("{}_{}", get_hostname().unwrap(), process::id());

        if config.enable_push_gateway {
            builder = builder
                .with_push_gateway(
                    &config.reporter_gw_endpoint,
                    Duration::from_secs(config.reporter_interval_secs),
                    Option::from(config.reporter_gw_username.clone()),
                    Option::from(config.reporter_gw_password.clone()),
                )
                .expect("push gateway endpoint should be valid");
        }

        builder
            .add_global_label(JOB_NAME_TAG, config.base.job_name.clone())
            .add_global_label(INSTANCE_TAG, instance.as_str())
            .idle_timeout(
                MetricKindMask::COUNTER | MetricKindMask::HISTOGRAM,
                Some(Duration::from_secs(config.reporter_interval_secs)),
            )
            .install()
            .expect("failed to install Prometheus recorder")
    }
}
