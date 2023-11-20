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

use std::sync::atomic::{AtomicU16, Ordering};

use dashmap::DashMap;
use itertools::Itertools;

use crate::{api::graph::graph_info_util::LABEL_MASK, convert_usize_to_u16, log_util::info};

const DELIMITER: char = '\u{0001}';

pub struct LabelDict {
    label_to_type: DashMap<String, u16>,
    type_to_label: DashMap<u16, String>,
    id: AtomicU16,
}

impl Default for LabelDict {
    fn default() -> Self {
        LabelDict {
            label_to_type: DashMap::default(),
            type_to_label: DashMap::default(),
            id: AtomicU16::new(0),
        }
    }
}

impl LabelDict {
    // register label and return label type by ascending order.
    pub fn register(&self, label: &str) -> u16 {
        let get_next_id = || {
            let next_id = self.id.fetch_add(1, Ordering::SeqCst);
            assert!(
                (next_id as u64) <= LABEL_MASK,
                "label should not greater than 1023"
            );
            self.label_to_type.insert(String::from(label), next_id);
            self.type_to_label.insert(next_id, String::from(label));
            next_id
        };

        let label_type_option = self.label_to_type.get(label);
        match label_type_option {
            Some(label_type) => *label_type.value(),
            None => get_next_id(),
        }
    }

    pub fn get_label_type(&self, label: &str) -> Option<u16> {
        if let Some(entry) = self.label_to_type.get(label) {
            return Some(*entry.value());
        }

        None
    }

    pub fn get_label(&self, label_type: u16) -> Option<String> {
        if let Some(entry) = self.type_to_label.get(&label_type) {
            return Some(entry.value().clone());
        }

        None
    }

    pub fn snapshot(&self) -> String {
        let mut list: Vec<(String, u16)> = Vec::new();

        for entry in &self.label_to_type {
            let str = format!("{}{}{}", entry.key(), DELIMITER, *entry.value());
            list.push((str, *entry.value()));
        }
        list.sort_by(|a, b| a.1.cmp(&b.1));
        let current_id = self.id.load(Ordering::SeqCst);
        info!("snapshot label dict with id {}", current_id);
        list.iter().map(|tuple| &tuple.0).join(";")
    }

    pub fn recover(&self, snapshot_dict: &str) {
        if snapshot_dict.is_empty() {
            return;
        }

        self.clear();

        let list: Vec<&str> = snapshot_dict.split(';').collect();
        for str in list.iter() {
            let values: Vec<&str> = str.split(DELIMITER).collect();
            self.register(values[0]);
            assert_eq!(
                values[1].parse::<u16>().unwrap(),
                self.get_label_type(values[0]).unwrap()
            );
        }

        let id = self.len();
        self.id.store(convert_usize_to_u16!(id), Ordering::SeqCst);
        let current_id = self.id.load(Ordering::SeqCst);
        info!("recovered label dict with id {}", current_id);
    }

    pub fn clear(&self) {
        self.id.store(0, Ordering::SeqCst);
        self.label_to_type.clear();
        self.type_to_label.clear();
    }

    pub fn len(&self) -> usize {
        self.label_to_type.len()
    }

    pub fn is_empty(&self) -> bool {
        self.label_to_type.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{atomic::Ordering, Arc},
        thread,
        time::Duration,
    };

    use crate::engine::dict::label_dict::LabelDict;

    #[test]
    fn test_dict() {
        let dict = Arc::new(LabelDict::default());
        gen_and_check_dict_data(&dict);

        let snapshot = dict.snapshot();
        dict.clear();
        assert_eq!(dict.id.load(Ordering::SeqCst), 0);
        assert!(dict.is_empty());

        dict.recover(&snapshot);
        assert_eq!(dict.id.load(Ordering::SeqCst), 1000);
        assert_eq!(dict.len(), 1000);
        check_dict_data(&dict);

        thread::sleep(Duration::from_secs(3));
    }

    fn gen_and_check_dict_data(dict: &LabelDict) {
        for i in 0..1000 {
            let key = format!("key-{}", i);

            let id1 = dict.register(&key);
            assert_eq!(id1, i);
        }
        check_dict_data(dict);
    }

    fn check_dict_data(dict: &LabelDict) {
        for i in 0..1000 {
            let key = format!("key-{}", i);
            let id2 = dict.get_label_type(&key);
            assert!(id2.is_some());
            assert_eq!(id2.unwrap(), i);

            let origin_key = dict.get_label(i);
            assert!(origin_key.is_some());
            assert_eq!(origin_key.unwrap(), key);
        }
    }
}
