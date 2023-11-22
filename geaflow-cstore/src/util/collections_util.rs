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

#![allow(unused)]
use std::hash::Hash;

use crossbeam::queue::SegQueue;
use dashmap::DashMap;

pub struct EvictingQueue<T>
where
    T: Eq + PartialEq + Hash + Clone + Copy,
{
    queue: SegQueue<T>,
    key_count: DashMap<T, u32>,
    capacity: usize,
}

impl<T> EvictingQueue<T>
where
    T: Eq + PartialEq + Hash + Clone + Copy,
{
    pub fn new(capacity: usize) -> Self {
        Self {
            queue: SegQueue::new(),
            key_count: DashMap::default(),
            capacity,
        }
    }

    pub fn push(&self, item: T) {
        if self.queue.len() >= self.capacity {
            let t = self.queue.pop().unwrap();
            *self.key_count.get_mut(&t).unwrap() -= 1;
        }
        self.queue.push(item);
        *self.key_count.entry(item).or_default() += 1;
    }

    pub fn calculate_specific_element_proportion(&self, specific_element: T) -> f32 {
        (*self.key_count.entry(specific_element).or_default() * 100) as f32
            / self.queue.len() as f32
    }
}
