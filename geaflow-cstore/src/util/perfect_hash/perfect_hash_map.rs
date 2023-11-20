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
    collections::{BTreeMap, HashMap},
    usize,
};

use crc32fast::Hasher;

use super::perfect_hash_function::{
    data_format::HashEntry,
    hash_builder::{
        HashBuilder, SIZE_OF_MULTIPLIER_PAIR, SIZE_OF_U32, SIZE_OF_USIZE, SIZE_OF_USIZE_TUPLE,
    },
};
use crate::log_util::info;

const CHECKSUM_LENGTH: usize = 4;

const RATIO: f64 = 1.1;
const RATIO_STEP: f64 = 0.05;
const AVG_PER_BUCKET_COUNT: u32 = 4;

trait MapType<K, V> {
    fn insert(&mut self, k: K, v: V) -> Option<V>;
    fn get(&self, k: &K) -> Option<&V>;
    fn iter(&self) -> Box<dyn Iterator<Item = (&K, &V)> + '_>;
    fn len(&self) -> usize;
}

impl<K: Eq + std::hash::Hash, V> MapType<K, V> for HashMap<K, V> {
    fn insert(&mut self, k: K, v: V) -> Option<V> {
        self.insert(k, v)
    }
    fn get(&self, k: &K) -> Option<&V> {
        self.get(k)
    }
    fn iter(&self) -> Box<dyn Iterator<Item = (&K, &V)> + '_> {
        Box::new(self.iter())
    }
    fn len(&self) -> usize {
        self.len()
    }
}

impl<K: Ord, V> MapType<K, V> for BTreeMap<K, V> {
    fn insert(&mut self, k: K, v: V) -> Option<V> {
        self.insert(k, v)
    }
    fn get(&self, k: &K) -> Option<&V> {
        self.get(k)
    }
    fn iter(&self) -> Box<dyn Iterator<Item = (&K, &V)> + '_> {
        Box::new(self.iter())
    }
    fn len(&self) -> usize {
        self.len()
    }
}

pub struct PerfectHashMap {
    hash_builder: HashBuilder,
    keys: Vec<u8>,
    values: Vec<u8>,
    value_offset: Vec<(usize, usize)>,
    key_offset: Vec<(usize, usize)>,

    key_start: Vec<usize>,
    value_start: Vec<usize>,
    ordered: bool,
}

impl Default for PerfectHashMap {
    fn default() -> Self {
        Self::new()
    }
}

impl PerfectHashMap {
    pub fn new() -> PerfectHashMap {
        PerfectHashMap {
            hash_builder: HashBuilder::new(),
            keys: vec![],
            values: vec![],
            value_offset: vec![],
            key_offset: vec![],
            key_start: vec![],
            value_start: vec![],
            ordered: false,
        }
    }

    pub fn set_ordered(mut self) -> PerfectHashMap {
        self.ordered = true;
        self
    }

    pub fn build_from_map(mut self, source: &HashMap<Vec<u8>, Vec<u8>>) -> Option<Self> {
        if source.is_empty() {
            info!("[perfect_hash: build_from_map] empty input");
            return None;
        }
        // if should sort, turn into treemap
        if self.ordered {
            return Self::inner_build(
                source
                    .iter()
                    .map(|(k, v)| (k.as_slice(), v.as_slice()))
                    .collect::<BTreeMap<&[u8], &[u8]>>(),
                true,
            );
        };

        let mut hash_entries: Vec<HashEntry> = vec![];
        let mut data_size: usize = 0;
        let mut key_size: usize = 0;

        // calculate hash for keys and space for values
        hash_entries.reserve(source.len());
        for kv in source {
            hash_entries.push(HashEntry::new().create_from_key(kv.0));
            key_size += kv.0.len();
            data_size += kv.1.len();
        }

        // construct perfect hash
        if !self.hash_builder.run_perfect(
            &mut hash_entries,
            RATIO,
            RATIO_STEP,
            AVG_PER_BUCKET_COUNT,
        ) {
            info!("[perfect_hash: build_from_map] build failed");
            return None;
        }

        // construct data and offset
        self.value_offset
            .resize(self.hash_builder.get_slot_count() as usize, (0, 0));
        self.key_offset
            .resize(self.hash_builder.get_slot_count() as usize, (0, 0));
        self.keys.reserve(key_size);
        self.values.reserve(data_size);
        let mut cur_value_offset: usize = 0;
        let mut cur_key_offset: usize = 0;
        for (i, kv) in source.iter().enumerate() {
            let slot = self.hash_builder.get_slot_index(hash_entries[i]) as usize;
            self.value_offset[slot] = (cur_value_offset, cur_value_offset + kv.1.len());
            self.key_offset[slot] = (cur_key_offset, cur_key_offset + kv.0.len());
            cur_value_offset += kv.1.len();
            cur_key_offset += kv.0.len();
            self.keys.extend(kv.0);
            self.values.extend(kv.1);
        }
        Some(self)
    }

    pub fn build_from_vec(self, source: &[(Vec<u8>, Vec<u8>)]) -> Option<Self> {
        if self.ordered {
            Self::inner_build(
                source
                    .iter()
                    .map(|(k, v)| (k.as_slice(), v.as_slice()))
                    .collect::<BTreeMap<&[u8], &[u8]>>(),
                true,
            )
        } else {
            Self::inner_build(
                source
                    .iter()
                    .map(|(k, v)| (k.as_slice(), v.as_slice()))
                    .collect::<HashMap<&[u8], &[u8]>>(),
                false,
            )
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        // calculate slot
        let slot = self
            .hash_builder
            .get_slot_index(HashEntry::new().create_from_key(key)) as usize;

        // get offset
        let mut offset = &self.key_offset[slot];
        if key.is_empty()
            || offset.1 - offset.0 != key.len()
            || self.keys[offset.0..offset.1] != key[..]
        {
            info!("[perfect_hash: get] key not found");
            return None;
        };

        // get value
        offset = &self.value_offset[slot];
        Some(&self.values[offset.0..offset.1])
    }

    pub fn serialize(self) -> Vec<u8> {
        // header
        let mut all_data: Vec<u8> = vec![0; CHECKSUM_LENGTH];

        // hash builder
        all_data.extend(self.hash_builder.get_binary_context());

        // offset of map for every slot
        let mut offset: Vec<u8> = self
            .key_offset
            .into_iter()
            .flat_map(|(a, b)| vec![a, b])
            .flat_map(|n: usize| n.to_le_bytes().to_vec())
            .collect();
        all_data.extend(offset);

        offset = self
            .value_offset
            .into_iter()
            .flat_map(|(a, b)| vec![a, b])
            .flat_map(|n: usize| n.to_le_bytes().to_vec())
            .collect();
        all_data.extend(offset);

        // key and data
        all_data.extend(self.keys.len().to_le_bytes().to_vec());
        all_data.extend(self.values.len().to_le_bytes().to_vec());
        all_data.extend(self.keys);
        all_data.extend(self.values);
        if self.ordered {
            let mut offset: Vec<u8> = self
                .key_start
                .into_iter()
                .flat_map(|n: usize| n.to_le_bytes().to_vec())
                .collect();
            all_data.extend(offset);
            offset = self
                .value_start
                .into_iter()
                .flat_map(|n: usize| n.to_le_bytes().to_vec())
                .collect();
            all_data.extend(offset);
        };
        let mut hasher = Hasher::new();
        hasher.update(&all_data[CHECKSUM_LENGTH..]);
        all_data[..CHECKSUM_LENGTH].copy_from_slice(hasher.finalize().to_le_bytes().as_slice());
        all_data
    }

    pub fn deserialize(mut self, binary_data: Vec<u8>) -> Option<Self> {
        // check header
        let mut hasher = Hasher::new();
        hasher.update(&binary_data[CHECKSUM_LENGTH..]);
        if u32::from_le_bytes(binary_data[..CHECKSUM_LENGTH].try_into().unwrap())
            != hasher.finalize()
        {
            info!("[perfect_hash: deserialize] checksum mismatch");
            return None;
        };

        // bucket count and slot count
        let mut offset: usize = CHECKSUM_LENGTH;
        let bucket_count = u32::from_le_bytes(
            binary_data[offset..offset + SIZE_OF_U32]
                .try_into()
                .unwrap(),
        ) as usize;
        offset += SIZE_OF_U32;
        let slots_count = u32::from_le_bytes(
            binary_data[offset..offset + SIZE_OF_U32]
                .try_into()
                .unwrap(),
        ) as usize;
        offset += SIZE_OF_U32;
        let multipliers_count = u32::from_le_bytes(
            binary_data[offset..offset + SIZE_OF_U32]
                .try_into()
                .unwrap(),
        ) as usize;
        offset += SIZE_OF_U32;
        if binary_data.len()
            < offset
                + (bucket_count * SIZE_OF_U32
                    + multipliers_count * SIZE_OF_MULTIPLIER_PAIR
                    + slots_count * SIZE_OF_USIZE_TUPLE * 2)
        {
            info!("[perfect_hash: deserialize] binary len not match");
            return None;
        };

        // multiplier index
        let multiplier_index: Vec<u32> = binary_data[offset..offset + SIZE_OF_U32 * bucket_count]
            .chunks_exact(SIZE_OF_U32)
            .map(|chunk| u32::from_le_bytes(chunk.try_into().unwrap()))
            .collect();
        offset += bucket_count * SIZE_OF_U32;

        // multipliers
        let multipliers: Vec<(u32, u32)> = binary_data
            [offset..offset + SIZE_OF_MULTIPLIER_PAIR * multipliers_count]
            .chunks_exact(SIZE_OF_MULTIPLIER_PAIR)
            .map(|chunk| {
                (
                    u32::from_le_bytes(chunk[0..4].try_into().unwrap()),
                    u32::from_le_bytes(chunk[4..8].try_into().unwrap()),
                )
            })
            .collect();
        offset += multipliers_count * SIZE_OF_MULTIPLIER_PAIR;

        // construct perfect hash
        self.hash_builder.build_context(
            bucket_count as u32,
            slots_count as u32,
            multiplier_index,
            multipliers,
        );

        // copy map offset
        self.key_offset = binary_data[offset..offset + SIZE_OF_USIZE_TUPLE * slots_count]
            .chunks_exact(SIZE_OF_USIZE_TUPLE)
            .map(|chunk| {
                (
                    usize::from_le_bytes(chunk[0..8].try_into().unwrap()),
                    usize::from_le_bytes(chunk[8..16].try_into().unwrap()),
                )
            })
            .collect();
        offset += slots_count * SIZE_OF_USIZE_TUPLE;

        self.value_offset = binary_data[offset..offset + SIZE_OF_USIZE_TUPLE * slots_count]
            .chunks_exact(SIZE_OF_USIZE_TUPLE)
            .map(|chunk| {
                (
                    usize::from_le_bytes(chunk[0..SIZE_OF_USIZE].try_into().unwrap()),
                    usize::from_le_bytes(
                        chunk[SIZE_OF_USIZE..SIZE_OF_USIZE * 2].try_into().unwrap(),
                    ),
                )
            })
            .collect();
        offset += slots_count * SIZE_OF_USIZE_TUPLE;

        // key length and value length
        let keys_length = usize::from_le_bytes(binary_data[offset..offset + 8].try_into().unwrap());
        offset += SIZE_OF_USIZE;
        let values_length = usize::from_le_bytes(
            binary_data[offset..offset + SIZE_OF_USIZE]
                .try_into()
                .unwrap(),
        );
        offset += SIZE_OF_USIZE;

        if binary_data.len() < (offset + keys_length + values_length) {
            info!("[perfect_hash: deserialize] binary len not match");
            return None;
        };

        // copy key and value data
        self.keys = binary_data[offset..offset + keys_length].to_vec();
        offset += keys_length;

        self.values = binary_data[offset..offset + values_length].to_vec();
        offset += values_length;

        // if is ordered, copy ordered offset
        if self.ordered && offset != binary_data.len() {
            self.key_start = binary_data[offset..(binary_data.len() + offset) / 2]
                .chunks_exact(SIZE_OF_USIZE)
                .map(|chunk| usize::from_le_bytes(chunk.try_into().unwrap()))
                .collect();
            self.value_start = binary_data[(binary_data.len() + offset) / 2..]
                .chunks_exact(SIZE_OF_USIZE)
                .map(|chunk| usize::from_le_bytes(chunk.try_into().unwrap()))
                .collect();
        };
        Some(self)
    }

    pub fn get_mem_usage(&self) -> usize {
        let mut total_size: usize = 0;
        total_size += self.hash_builder.get_mem_usage();
        total_size += self.value_offset.capacity() * SIZE_OF_USIZE_TUPLE * 2;
        total_size += self.key_start.capacity() * SIZE_OF_USIZE * 2;
        total_size += self.values.capacity() + self.keys.capacity();

        total_size
    }

    pub fn iter(&self) -> PerfectHashMapIterator {
        PerfectHashMapIterator::new(self)
    }

    pub fn keys(&self) -> PerfectHashKeyIterator {
        PerfectHashKeyIterator {
            inner: PerfectHashMapIterator::new(self),
        }
    }

    pub fn values(&self) -> PerfectHashValueIterator {
        PerfectHashValueIterator {
            inner: PerfectHashMapIterator::new(self),
        }
    }

    fn inner_build<'a, T: MapType<&'a [u8], &'a [u8]>>(
        kv_map: T,
        should_sort: bool,
    ) -> Option<Self> {
        if kv_map.len() == 0 {
            info!("[perfect_hash: inner_build] empty input");
            return None;
        }
        let mut perfect_map = PerfectHashMap::new();
        perfect_map.ordered = should_sort;
        let mut hash_entries: Vec<HashEntry> = vec![];
        let mut data_size: usize = 0;
        let mut key_size: usize = 0;

        hash_entries.reserve(kv_map.len());

        for kv in kv_map.iter() {
            hash_entries.push(HashEntry::new().create_from_key(kv.0));
            key_size += kv.0.len();
            data_size += kv.1.len();
        }

        // construct perfect hash
        if !perfect_map.hash_builder.run_perfect(
            &mut hash_entries,
            RATIO,
            RATIO_STEP,
            AVG_PER_BUCKET_COUNT,
        ) {
            info!("[perfect_hash: inner_merge]build failed");
            return None;
        };

        // construct data and offset
        perfect_map
            .value_offset
            .resize(perfect_map.hash_builder.get_slot_count() as usize, (0, 0));
        perfect_map
            .key_offset
            .resize(perfect_map.hash_builder.get_slot_count() as usize, (0, 0));

        if should_sort {
            perfect_map.key_start.reserve(kv_map.len() + 1);
            perfect_map.value_start.reserve(kv_map.len() + 1);
        }

        perfect_map.keys.reserve(key_size);
        perfect_map.values.reserve(data_size);
        let mut cur_value_offset: usize = 0;
        let mut cur_key_offset: usize = 0;
        for (i, kv) in kv_map.iter().enumerate() {
            let slot = perfect_map.hash_builder.get_slot_index(hash_entries[i]) as usize;
            perfect_map.key_offset[slot] = (cur_key_offset, cur_key_offset + kv.0.len());
            perfect_map.value_offset[slot] = (cur_value_offset, cur_value_offset + kv.1.len());
            if should_sort {
                perfect_map.key_start.push(cur_key_offset);
                perfect_map.value_start.push(cur_value_offset);
            };
            cur_key_offset += kv.0.len();
            cur_value_offset += kv.1.len();
            perfect_map.keys.extend(*kv.0);
            perfect_map.values.extend(*kv.1);
        }
        if should_sort {
            perfect_map.key_start.push(cur_key_offset);
            perfect_map.value_start.push(cur_value_offset);
        };

        Some(perfect_map)
    }

    pub fn merge_one(self, other: PerfectHashMap) -> Option<Self> {
        // fetch kv in self and other and put reference in a hashmap
        let mut kv_map: HashMap<&[u8], &[u8]> = HashMap::new();
        for i in 0..self.hash_builder.get_slot_count() as usize {
            let key_offset = self.key_offset[i];
            if key_offset.1 != key_offset.0 {
                let value_offset = self.value_offset[i];
                kv_map.insert(
                    &self.keys[key_offset.0..key_offset.1],
                    &self.values[value_offset.0..value_offset.1],
                );
            };
        }

        for i in 0..other.hash_builder.get_slot_count() as usize {
            let key_offset = other.key_offset[i];
            if key_offset.1 != key_offset.0 {
                let value_offset = other.value_offset[i];
                kv_map.insert(
                    &other.keys[key_offset.0..key_offset.1],
                    &other.values[value_offset.0..value_offset.1],
                );
            };
        }
        Self::inner_build(kv_map, false)
    }

    pub fn merge_one_and_sort(self, other: PerfectHashMap) -> Option<Self> {
        // fetch kv in self and other and put reference in a btreemap
        let mut kv_map: BTreeMap<&[u8], &[u8]> = BTreeMap::new();
        for i in 0..self.hash_builder.get_slot_count() as usize {
            let key_offset = self.key_offset[i];
            if key_offset.1 != key_offset.0 {
                let value_offset = self.value_offset[i];
                kv_map.insert(
                    &self.keys[key_offset.0..key_offset.1],
                    &self.values[value_offset.0..value_offset.1],
                );
            };
        }

        for i in 0..other.hash_builder.get_slot_count() as usize {
            let key_offset = other.key_offset[i];
            if key_offset.1 != key_offset.0 {
                let value_offset = other.value_offset[i];
                kv_map.insert(
                    &other.keys[key_offset.0..key_offset.1],
                    &other.values[value_offset.0..value_offset.1],
                );
            };
        }
        Self::inner_build(kv_map, true)
    }

    pub fn merge_multi(others: &[&PerfectHashMap]) -> Option<Self> {
        // fetch kv in others and put reference in a hashmap
        let mut kv_map: HashMap<&[u8], &[u8]> = HashMap::new();
        for other in others {
            for i in 0..other.hash_builder.get_slot_count() as usize {
                let key_offset = other.key_offset[i];
                if key_offset.1 != key_offset.0 {
                    let value_offset = other.value_offset[i];
                    kv_map.insert(
                        &other.keys[key_offset.0..key_offset.1],
                        &other.values[value_offset.0..value_offset.1],
                    );
                };
            }
        }
        Self::inner_build(kv_map, false)
    }

    pub fn merge_multi_and_sort(others: &[&PerfectHashMap]) -> Option<Self> {
        // fetch kv in others and put reference in a btreemap
        let mut kv_map: BTreeMap<&[u8], &[u8]> = BTreeMap::new();
        for other in others {
            for i in 0..other.hash_builder.get_slot_count() as usize {
                let key_offset = other.key_offset[i];
                if key_offset.1 != key_offset.0 {
                    let value_offset = other.value_offset[i];
                    kv_map.insert(
                        &other.keys[key_offset.0..key_offset.1],
                        &other.values[value_offset.0..value_offset.1],
                    );
                };
            }
        }
        Self::inner_build(kv_map, true)
    }
}

type InValidFn = dyn Fn(&PerfectHashMapIterator) -> bool;
type GetOffsetFn = dyn Fn(&PerfectHashMapIterator) -> (usize, usize);

pub struct PerfectHashMapIterator<'a> {
    map: &'a PerfectHashMap,
    cur_index: usize,

    upper: usize,
    in_valid: Box<InValidFn>,
    get_key_offset: Box<GetOffsetFn>,
    get_value_offset: Box<GetOffsetFn>,
}

impl<'a> PerfectHashMapIterator<'a> {
    pub fn new(m: &'a PerfectHashMap) -> Self {
        let mut iter = PerfectHashMapIterator {
            map: m,
            cur_index: 0,
            upper: 0,
            in_valid: Box::new(|_iter: &PerfectHashMapIterator| -> bool { true }),
            get_key_offset: Box::new(|_iter: &PerfectHashMapIterator| -> (usize, usize) { (0, 0) }),
            get_value_offset: Box::new(|_iter: &PerfectHashMapIterator| -> (usize, usize) {
                (0, 0)
            }),
        };
        let map = iter.map;

        if map.ordered && !map.key_start.is_empty() {
            iter.upper = map.key_start.len() - 1;
            iter.in_valid = Box::new(|iter: &PerfectHashMapIterator| -> bool {
                iter.map.key_start[iter.cur_index] == iter.map.key_start[iter.cur_index + 1]
            });
            iter.get_key_offset = Box::new(|iter: &PerfectHashMapIterator| -> (usize, usize) {
                (
                    iter.map.key_start[iter.cur_index],
                    iter.map.key_start[iter.cur_index + 1],
                )
            });
            iter.get_value_offset = Box::new(|iter: &PerfectHashMapIterator| -> (usize, usize) {
                (
                    iter.map.value_start[iter.cur_index],
                    iter.map.value_start[iter.cur_index + 1],
                )
            });
        } else {
            iter.upper = map.key_offset.len();
            iter.in_valid = Box::new(|iter: &PerfectHashMapIterator| -> bool {
                iter.map.key_offset[iter.cur_index].0 == iter.map.key_offset[iter.cur_index].1
            });
            iter.get_key_offset = Box::new(|iter: &PerfectHashMapIterator| -> (usize, usize) {
                iter.map.key_offset[iter.cur_index]
            });
            iter.get_value_offset = Box::new(|iter: &PerfectHashMapIterator| -> (usize, usize) {
                iter.map.value_offset[iter.cur_index]
            });
        };
        iter
    }
}

impl<'a> Iterator for PerfectHashMapIterator<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        let map = self.map;

        if self.cur_index < self.upper {
            while (self.in_valid)(self) {
                self.cur_index += 1;
                if self.cur_index == self.upper {
                    return None;
                }
            }
            let key_offset: (usize, usize) = (self.get_key_offset)(self);
            let value_offset: (usize, usize) = (self.get_value_offset)(self);
            let res = Some((
                &map.keys[key_offset.0..key_offset.1],
                &map.values[value_offset.0..value_offset.1],
            ));
            self.cur_index += 1;
            res
        } else {
            None
        }
    }
}

impl<'a> IntoIterator for &'a PerfectHashMap {
    type Item = (&'a [u8], &'a [u8]);
    type IntoIter = PerfectHashMapIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        PerfectHashMapIterator::new(self)
    }
}

pub struct PerfectHashKeyIterator<'a> {
    inner: PerfectHashMapIterator<'a>,
}

impl<'a> Iterator for PerfectHashKeyIterator<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some((k, _)) => Some(k),
            None => None,
        }
    }
}

pub struct PerfectHashValueIterator<'a> {
    inner: PerfectHashMapIterator<'a>,
}

impl<'a> Iterator for PerfectHashValueIterator<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some((_, v)) => Some(v),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::{thread_rng, Rng};

    use super::*;

    const KEY_SIZE: usize = 32;
    const KEY_COUNT: usize = 100000;
    const VALUE_SIZE: usize = 8;

    #[test]
    fn build_from_map_test() {
        let mut key_buf: Vec<u8> = vec![0; KEY_SIZE];
        let mut source: HashMap<Vec<u8>, Vec<u8>> = HashMap::default();
        let mut btreemap: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

        // generate kv
        while source.len() < KEY_COUNT {
            for i in 0..KEY_SIZE {
                key_buf[i] = thread_rng().gen::<u8>();
            }
            let value = vec![thread_rng().gen::<u8>(); VALUE_SIZE];
            source.insert(key_buf.clone(), value.clone());
            btreemap.insert(key_buf.clone(), value.clone());
        }
        let backup = source.clone();

        let mut hash_map = PerfectHashMap::new().build_from_map(&source).unwrap();

        for kv in &backup {
            assert_eq!(kv.1.as_slice(), hash_map.get(&kv.0).unwrap());
        }

        hash_map = PerfectHashMap::new()
            .set_ordered()
            .build_from_map(&source)
            .unwrap();

        let mut iter = btreemap.iter();
        for kv in &hash_map {
            assert_eq!(kv.0, iter.next().unwrap().0);
        }
    }

    #[test]
    fn build_from_vec_test() {
        let mut key_buf: Vec<u8> = vec![0; KEY_SIZE];
        let mut source: Vec<(Vec<u8>, Vec<u8>)> = vec![];
        let mut btreemap: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

        // generate kv
        while source.len() < KEY_COUNT {
            for i in 0..KEY_SIZE {
                key_buf[i] = thread_rng().gen::<u8>();
            }
            let value = vec![thread_rng().gen::<u8>(); VALUE_SIZE];
            source.push((key_buf.clone(), value.clone()));
            btreemap.insert(key_buf.clone(), value.clone());
        }
        let backup = source.clone();

        let mut hash_map = PerfectHashMap::new().build_from_vec(&source).unwrap();

        for kv in &backup {
            assert_eq!(kv.1, hash_map.get(&kv.0).unwrap());
        }
        hash_map = PerfectHashMap::new()
            .set_ordered()
            .build_from_vec(&source)
            .unwrap();

        let mut iter = btreemap.iter();
        for kv in &hash_map {
            assert_eq!(kv.0, iter.next().unwrap().0);
        }
    }

    #[test]
    fn deserialize_test() {
        let mut key_buf: Vec<u8> = vec![0; KEY_SIZE];
        let mut source: HashMap<Vec<u8>, Vec<u8>> = HashMap::default();

        // generate kv
        while source.len() < KEY_COUNT {
            for i in 0..KEY_SIZE {
                key_buf[i] = thread_rng().gen::<u8>();
            }
            source.insert(key_buf.clone(), vec![thread_rng().gen::<u8>(); VALUE_SIZE]);
        }
        let backup = source.clone();

        let mut hash_map = PerfectHashMap::new().build_from_map(&source).unwrap();
        let binary = hash_map.serialize();
        hash_map = PerfectHashMap::new().deserialize(binary).unwrap();

        for kv in backup {
            assert_eq!(kv.1.as_slice(), hash_map.get(&kv.0).unwrap());
        }
    }

    #[test]
    fn merge_one_test() {
        let mut key_buf: Vec<u8> = vec![0; KEY_SIZE];
        let mut source: Vec<HashMap<Vec<u8>, Vec<u8>>> = vec![HashMap::default(); 2];
        let mut all: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

        // generate kv
        for j in 0..2 {
            while source[j].len() < KEY_COUNT {
                for i in 0..KEY_SIZE {
                    key_buf[i] = thread_rng().gen::<u8>();
                }
                let value = vec![thread_rng().gen::<u8>(); VALUE_SIZE];
                source[j].insert(key_buf.clone(), value.clone());
                all.insert(key_buf.clone(), value.clone());
            }
        }

        let hash_map1 = PerfectHashMap::new().build_from_map(&source[0]).unwrap();
        let hash_map2 = PerfectHashMap::new().build_from_map(&source[1]).unwrap();

        let merged = hash_map1.merge_one_and_sort(hash_map2).unwrap();

        for kv in &all {
            assert_eq!(kv.1.as_slice(), merged.get(&kv.0).unwrap());
        }

        // test ordered merge
        let mut iter = all.iter();
        for kv in &merged {
            assert_eq!(kv.0, iter.next().unwrap().0);
        }
    }

    #[test]
    fn merge_multi_test() {
        const MAP_NUM: usize = 5;
        let mut key_buf: Vec<u8> = vec![0; KEY_SIZE];
        let mut source: Vec<HashMap<Vec<u8>, Vec<u8>>> = vec![HashMap::default(); MAP_NUM];
        let mut all: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

        // generate kv
        for j in 0..MAP_NUM {
            while source[j].len() < KEY_COUNT / MAP_NUM {
                for i in 0..KEY_SIZE {
                    key_buf[i] = thread_rng().gen::<u8>();
                }
                let value = vec![thread_rng().gen::<u8>(); VALUE_SIZE];
                source[j].insert(key_buf.clone(), value.clone());
                all.insert(key_buf.clone(), value.clone());
            }
        }

        let mut hash_maps: Vec<PerfectHashMap> = vec![];
        let mut hash_map_refs: Vec<&PerfectHashMap> = vec![];

        for i in 0..MAP_NUM {
            hash_maps.push(PerfectHashMap::new().build_from_map(&source[i]).unwrap());
        }

        for i in 0..MAP_NUM {
            hash_map_refs.push(&hash_maps[i]);
        }

        let merged = PerfectHashMap::merge_multi_and_sort(&hash_map_refs).unwrap();

        for kv in &all {
            assert_eq!(kv.1.as_slice(), merged.get(&kv.0).unwrap());
        }

        // test ordered merge
        let mut iter = all.iter();
        for kv in &merged {
            assert_eq!(kv.0, iter.next().unwrap().0);
        }
    }
}
