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

#![allow(unused_variables)]

use std::{cmp::max, mem::size_of};

use bytes::BufMut;
use rand::{thread_rng, Rng};

use super::{
    bitmap::Bitmap,
    data_format::{HashEntry, UInt64Struct},
};

const K_MIN_SLOT_COUNT: u32 = 1000;
const K_HASH_ENTRIES_LOW_BOUND: usize = 1000;
const K_HASH_ENTRIES_MID_BOUND: usize = 5000;
const K_HASH_ENTRIES_UP_BOUND: usize = 10000;

const MAX_HASH_BUILDER_RETRY_TIMES: u32 = 100;
const HASH_BUILDER_MAX_NUM_OF_ENTRIES_IN_SEGMENT: usize = 255;
const MAX_COMPUTE_ONE_MULTIPLIER_RETRY_TIMES: u32 = 100000;
const MAX_COMPUTE_ONE_MULTIPLIER_RETRY_STEP: u32 = 0;

const META_LENGTH: usize = 3;
const MULTIPLIERS_ESTIMATE: usize = 2000; // 100w key

pub const SIZE_OF_U32: usize = size_of::<u32>();
pub const SIZE_OF_U64: usize = size_of::<u64>();
pub const SIZE_OF_USIZE: usize = size_of::<usize>();
pub const SIZE_OF_USIZE_TUPLE: usize = size_of::<(usize, usize)>();
pub const SIZE_OF_MULTIPLIER_PAIR: usize = size_of::<MultiplierPair>();

#[derive(Copy, Clone)]
struct MultiplierPair {
    alpha: u32,
    beta: u32,
}
impl MultiplierPair {
    pub fn new(a: u32, b: u32) -> Self {
        MultiplierPair { alpha: a, beta: b }
    }
}

struct PhContext {
    bucket_count: u32,
    slots_count: u32,
    multipliers: Vec<MultiplierPair>,
    multiplier_indexes: Vec<u32>,
}
impl PhContext {
    pub fn new(slots_count: u32, bucket_count: u32) -> Self {
        let mut ph = PhContext {
            bucket_count,
            slots_count,
            multipliers: vec![],
            multiplier_indexes: vec![0; bucket_count as usize],
        };
        ph.multipliers.reserve(MULTIPLIERS_ESTIMATE);
        ph
    }

    pub fn get_mem_usage(&self) -> usize {
        let mut total_size: usize = 0;
        total_size += self.multipliers.len() * SIZE_OF_MULTIPLIER_PAIR;
        total_size += self.multiplier_indexes.len() * SIZE_OF_U32;
        total_size
    }
}

#[derive(Clone)]
struct HashEntrySegment {
    pub bucket_index: u32,
    pub entry_index: u32,
    pub entry_count: u32,
}

pub struct HashBuilder {
    max_retry_cycles: u32,
    max_compute_one_multiplier_retry_times: u32,
    ph_context: PhContext,
    hash_entry_segments: Vec<HashEntrySegment>,
    sorted_hashes: Vec<UInt64Struct>,
}

impl Default for HashBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl HashBuilder {
    pub fn new() -> Self {
        HashBuilder {
            max_retry_cycles: 0,
            max_compute_one_multiplier_retry_times: 0,
            ph_context: PhContext {
                bucket_count: 0,
                slots_count: 0,
                multipliers: vec![],
                multiplier_indexes: vec![],
            },
            hash_entry_segments: vec![],
            sorted_hashes: vec![],
        }
    }

    pub fn run_perfect(
        &mut self,
        hash_entries: &mut [HashEntry],
        ratio: f64,
        ratio_step: f64,
        avg_per_bucket_count: u32,
    ) -> bool {
        let mut retry_times = 0;
        let mut success;
        let mut target_ratio;
        let mut needs_retry;
        self.max_retry_cycles = 0;
        loop {
            // set retry times and target ratio
            // increase target ratio by ratio step every time retry
            self.max_compute_one_multiplier_retry_times = MAX_COMPUTE_ONE_MULTIPLIER_RETRY_TIMES
                + retry_times * MAX_COMPUTE_ONE_MULTIPLIER_RETRY_STEP;
            target_ratio =
                self.get_compute_ratio(ratio, ratio_step, retry_times, hash_entries.len());

            assert_ne!(avg_per_bucket_count, 0, "avgPerBucketCount is zero.");
            let slot_count = max(
                (target_ratio * (hash_entries.len() as f64)) as u32,
                K_MIN_SLOT_COUNT,
            );
            let bucket_count =
                (hash_entries.len() as u32 + avg_per_bucket_count - 1) / avg_per_bucket_count;

            self.ph_context = PhContext::new(slot_count, bucket_count);

            // map entries to bucket and sort bucket by descent entries
            self.sort(hash_entries);

            // compute perfect hash
            success = self.compute(hash_entries);

            needs_retry = !success && (retry_times < MAX_HASH_BUILDER_RETRY_TIMES);
            retry_times += 1;
            if !needs_retry {
                break;
            }
        }
        success
    }

    fn get_compute_ratio(
        &mut self,
        mut init_ratio: f64,
        mut ratio_step: f64,
        retry_times: u32,
        num_of_hashes: usize,
    ) -> f64 {
        if num_of_hashes <= K_HASH_ENTRIES_LOW_BOUND {
            init_ratio *= 1.7;
            ratio_step *= 4.0;
        } else if num_of_hashes <= K_HASH_ENTRIES_MID_BOUND {
            init_ratio *= 1.5;
            ratio_step *= 3.0;
        } else if num_of_hashes <= K_HASH_ENTRIES_UP_BOUND {
            init_ratio *= 1.3;
            ratio_step *= 2.0;
        };
        (init_ratio + ratio_step * retry_times as f64).max(1.1)
    }

    fn sort(&mut self, hashed_entries: &mut [HashEntry]) {
        let bucket_count = self.ph_context.bucket_count;
        if bucket_count == 0 {
            return;
        };
        self.hash_entry_segments.resize(
            bucket_count as usize,
            HashEntrySegment {
                bucket_index: 0,
                entry_index: 0,
                entry_count: 0,
            },
        );

        for segment in &mut self.hash_entry_segments {
            segment.entry_count = 0;
        }
        let mut bucket_offset = vec![0; bucket_count as usize];

        // Map hash_entry to bucket
        for entry in &mut *hashed_entries {
            let bucket_index = entry.hash.hash1.wrapping_add(entry.hash.hash2) % bucket_count;
            entry.bucket_index = bucket_index;
            self.hash_entry_segments[bucket_index as usize].entry_count += 1;
        }

        // bucket_offset: offset of each bucket's first entry
        bucket_offset[0] = 0;
        self.hash_entry_segments[0].entry_index = 0;
        self.hash_entry_segments[0].bucket_index = 0;
        for bucket_index in 1..bucket_count as usize {
            let entry_count = self.hash_entry_segments[bucket_index - 1].entry_count;
            bucket_offset[bucket_index] = bucket_offset[bucket_index - 1] + entry_count;
            self.hash_entry_segments[bucket_index].bucket_index = bucket_index as u32;
            self.hash_entry_segments[bucket_index].entry_index = bucket_offset[bucket_index];
        }

        // self.sorted_hashes = hashed_entries.hash sorted by bucket ID
        self.sorted_hashes
            .resize(hashed_entries.len(), UInt64Struct { hash1: 0, hash2: 0 });
        for entry in &mut *hashed_entries {
            self.sorted_hashes[bucket_offset[entry.bucket_index as usize] as usize] = entry.hash;
            bucket_offset[entry.bucket_index as usize] += 1;
        }

        // entry_segment sort by entry_count
        self.hash_entry_segments.sort_by(|a, b| {
            if a.entry_count == b.entry_count {
                a.entry_index.cmp(&b.entry_index)
            } else {
                b.entry_count.cmp(&a.entry_count)
            }
        });
    }

    fn compute(&mut self, hashed_entries: &[HashEntry]) -> bool {
        // construct bitmap for every slot
        let mut bitmap = Bitmap::new(self.ph_context.slots_count);
        let mut multiplier_index: u32 = 0;

        // compute perfect hash function for one bucket
        for i in 0..self.hash_entry_segments.len() {
            if self.hash_entry_segments[i].entry_count == 0 {
                continue;
            };
            let success =
                self.compute_segment_bucket_multiplier(i, &mut multiplier_index, &mut bitmap);
            if !success {
                return false;
            };
        }

        self.ph_context.multipliers.shrink_to_fit();
        self.sorted_hashes.clear();
        self.sorted_hashes.shrink_to_fit();
        self.hash_entry_segments.clear();
        self.hash_entry_segments.shrink_to_fit();

        bitmap.num_of_valid_bits() == hashed_entries.len() as u32
    }

    fn compute_segment_bucket_multiplier(
        &mut self,
        index: usize,
        current_multiplier_index: &mut u32,
        bitmap: &mut Bitmap,
    ) -> bool {
        // begin and end entry index for the input bucket
        let first_index = self.hash_entry_segments[index].entry_index as usize;
        let last_index = first_index + self.hash_entry_segments[index].entry_count as usize;

        // try previous multipliers
        for i in 0..*current_multiplier_index as usize {
            let alpha = self.ph_context.multipliers[i].alpha;
            let beta = self.ph_context.multipliers[i].beta;

            let success = self.try_one_multiplier(first_index, last_index, alpha, beta, bitmap);
            if success {
                self.ph_context.multiplier_indexes
                    [self.hash_entry_segments[index].bucket_index as usize] = i as u32;
                return true;
            };
        }

        // generate random alpha and beta,
        // and try to map every entry in the bucket to global slots without conflict
        // if success, append alpha and beta as a multiplier to the end of multipliers
        // and record the multiplier index for the current bucket in multiplier_indexes
        let mut retry_times = 0;
        let mut alpha: u32;
        let mut beta: u32;
        while retry_times < self.max_compute_one_multiplier_retry_times {
            retry_times += 1;
            alpha = thread_rng().gen::<u32>();
            beta = thread_rng().gen::<u32>();

            let success = self.try_one_multiplier(first_index, last_index, alpha, beta, bitmap);
            if success {
                self.ph_context.multiplier_indexes
                    [self.hash_entry_segments[index].bucket_index as usize] =
                    *current_multiplier_index;
                self.ph_context
                    .multipliers
                    .push(MultiplierPair::new(alpha, beta));
                *current_multiplier_index += 1;
                return true;
            };
        }

        self.max_retry_cycles = self.max_retry_cycles.max(retry_times);
        false
    }

    fn try_one_multiplier(
        &mut self,
        first_index: usize,
        last_index: usize,
        alpha: u32,
        beta: u32,
        bitmap: &mut Bitmap,
    ) -> bool {
        // slot_indexes: slots that have already used by entries within current bucket
        let mut slot_indexes: Vec<u32> =
            Vec::with_capacity(HASH_BUILDER_MAX_NUM_OF_ENTRIES_IN_SEGMENT);
        let slot_count = self.ph_context.slots_count;
        let mut slot: u32;

        // for every entry, slot index = (hash1 + hash2 * alpha + beta) % slot_count
        // if the slot has not been used by previous buckets(bitmap check) and current
        // bucket(slot_indexes check), use it.
        let get_perf_hash_slot =
            |alpha: &u32, beta: &u32, hash1: &u32, hash2: &u32, slot_count: &u32| -> u32 {
                hash1
                    .wrapping_add(alpha.wrapping_mul(*hash2))
                    .wrapping_add(*beta)
                    % slot_count
            };
        for current_entry in &self.sorted_hashes[first_index..last_index] {
            slot = get_perf_hash_slot(
                &alpha,
                &beta,
                &current_entry.hash1,
                &current_entry.hash2,
                &slot_count,
            );
            if bitmap.get(slot) {
                return false;
            };

            for i in &slot_indexes {
                if &slot == i {
                    return false;
                };
            }
            slot_indexes.push(slot);
        }

        // if success, add current slots to global bitmap
        for slot_index in &slot_indexes {
            bitmap.set(*slot_index);
        }
        true
    }

    pub fn get_slot_index(&self, entry: HashEntry) -> u32 {
        let ph_context = &self.ph_context;
        let bucket_index =
            entry.hash.hash1.wrapping_add(entry.hash.hash2) % ph_context.bucket_count;
        let multiplier_index = ph_context.multiplier_indexes[bucket_index as usize] as usize;
        let alpha = &ph_context.multipliers[multiplier_index].alpha;
        let beta = &ph_context.multipliers[multiplier_index].beta;
        entry
            .hash
            .hash1
            .wrapping_add(alpha.wrapping_mul(entry.hash.hash2))
            .wrapping_add(*beta)
            % ph_context.slots_count
    }

    pub fn get_slot_count(&self) -> u32 {
        self.ph_context.slots_count
    }

    pub fn get_binary_context(self) -> Vec<u8> {
        // ------------------------------
        // bucket_count
        // slot_count
        // multipliers_count
        // ------------------------------
        // multiplier_index0
        // multiplier_index1
        // ...
        // ------------------------------
        // multipliers0.alpha
        // multipliers0.beta
        // multipliers1.alpha
        // multipliers1.beta
        // ...
        // ------------------------------
        let bucket_count = self.ph_context.bucket_count as usize;
        let multipliers_count = self.ph_context.multipliers.len();
        let multiplier_index_len = bucket_count;
        let multiplier_len = multipliers_count * 2;
        let mut data = vec![0; META_LENGTH + multiplier_index_len + multiplier_len];
        data[0] = self.ph_context.bucket_count;
        data[1] = self.ph_context.slots_count;
        data[2] = multipliers_count as u32;
        data[META_LENGTH..(multiplier_index_len + META_LENGTH)]
            .copy_from_slice(self.ph_context.multiplier_indexes.as_slice());
        for i in 0..multipliers_count {
            data[META_LENGTH + multiplier_index_len + i * 2] = self.ph_context.multipliers[i].alpha;
            data[META_LENGTH + multiplier_index_len + i * 2 + 1] =
                self.ph_context.multipliers[i].beta;
        }
        let mut v_u8: Vec<u8> = Vec::with_capacity(data.len() * SIZE_OF_U32);
        for i in data {
            v_u8.put_slice(&i.to_le_bytes());
        }
        v_u8
    }

    pub fn build_context(
        &mut self,
        bucket_count: u32,
        slots_count: u32,
        multiplier_index: Vec<u32>,
        multipliers: Vec<(u32, u32)>,
    ) {
        self.ph_context.bucket_count = bucket_count;
        self.ph_context.slots_count = slots_count;
        self.ph_context.multiplier_indexes = multiplier_index;
        self.ph_context.multipliers.reserve(multipliers.len());
        for item in multipliers {
            self.ph_context
                .multipliers
                .push(MultiplierPair::new(item.0, item.1));
        }
    }

    pub fn get_mem_usage(&self) -> usize {
        self.ph_context.get_mem_usage()
    }
}
