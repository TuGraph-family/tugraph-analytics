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

use std::collections::HashMap;

use rustc_hash::FxHashMap;

use crate::{config::StoreConfig, engine::dict::label_dict::LabelDict};

const DEFAULT_TTL: i32 = -1;

pub struct TtlDict {
    ttl_dict: FxHashMap<u16, i32>,
    store_ttl: String,
}

#[cfg(test)]
impl Default for TtlDict {
    fn default() -> Self {
        Self {
            ttl_dict: FxHashMap::default(),
            store_ttl: String::new(),
        }
    }
}

impl TtlDict {
    pub fn new(label_dict: &LabelDict, store_config: &StoreConfig) -> Self {
        let mut ttl_dict = TtlDict {
            ttl_dict: FxHashMap::default(),
            store_ttl: store_config.store_ttl.clone(),
        };

        ttl_dict.init(label_dict);
        ttl_dict
    }

    fn init(&mut self, label_dict: &LabelDict) {
        // ttl string should be json format: {"label1": "86400", "label2": "86400"}
        let ttl_str = self.store_ttl.as_str();
        if !ttl_str.is_empty() {
            let ttl_json: HashMap<String, String> = serde_json::from_str(ttl_str).unwrap();
            for ttl_entry in &ttl_json {
                let label: &str = ttl_entry.0;
                let ttl: i32 = ttl_entry.1.trim().parse().expect("not a number");

                let label_type = if let Some(label_type_tmp) = label_dict.get_label_type(label) {
                    label_type_tmp
                } else {
                    label_dict.register(label)
                };

                self.ttl_dict.insert(label_type, ttl);
            }
        }
    }

    pub fn get_ttl(&self, label_type: u16) -> i32 {
        if let Some(ttl) = self.ttl_dict.get(&label_type) {
            return *ttl;
        }

        DEFAULT_TTL
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        engine::dict::{label_dict::LabelDict, ttl_dict::TtlDict},
        CStoreConfigBuilder, ConfigMap,
    };

    #[test]
    fn test_config() {
        let mut config_map = ConfigMap::default();
        config_map.insert(
            "store.store_ttl",
            r#"{"label1": "1234", "label2": "123456"}"#,
        );
        let cstore_config = CStoreConfigBuilder::default()
            .set_with_map(&config_map)
            .build();

        let label_dict = Arc::new(LabelDict::default());
        label_dict.register("label1");
        label_dict.register("label2");

        let ttl_dict = TtlDict::new(&label_dict, &cstore_config.store);
        assert_eq!(
            ttl_dict.get_ttl(label_dict.get_label_type("label1").unwrap()),
            1234
        );
        assert_eq!(
            ttl_dict.get_ttl(label_dict.get_label_type("label2").unwrap()),
            123456
        );
    }
}
