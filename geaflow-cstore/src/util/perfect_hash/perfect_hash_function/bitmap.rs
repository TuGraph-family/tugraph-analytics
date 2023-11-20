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

const BITS_OF_U8: u32 = 8;

pub struct Bitmap {
    pub map: Vec<u8>,
}

impl Bitmap {
    pub fn new(num_bits: u32) -> Self {
        Bitmap {
            map: vec![0; ((num_bits + BITS_OF_U8 - 1) >> 3) as usize],
        }
    }

    pub fn set(&mut self, idx: u32) {
        self.map[idx as usize >> 3] |= 1 << (idx & (BITS_OF_U8 - 1));
    }

    #[allow(dead_code)]
    pub fn unset(&mut self, idx: u32) {
        self.map[idx as usize >> 3] &= !(1 << (idx & (BITS_OF_U8 - 1)));
    }

    pub fn get(&self, idx: u32) -> bool {
        self.map[idx as usize >> 3] & (1 << (idx & (BITS_OF_U8 - 1))) != 0
    }

    pub fn num_of_valid_bits(&self) -> u32 {
        let mut count: u32 = 0;
        for i in 0..self.map.len() {
            count += Bitmap::count_bits(&self.map[i]);
        }
        count
    }

    fn count_bits(input: &u8) -> u32 {
        let mut x = !*input;
        let mut count: u32 = 0;
        while x != 0 {
            x &= x - 1;
            count += 1;
        }
        8 - count
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use rand::Rng;

    use super::*;

    #[test]
    fn test_bitmap() {
        let bit_count = 10000000;
        let mut bitmap = Bitmap::new(bit_count);
        let mut set: HashSet<u32> = HashSet::new();
        for i in 0..bit_count / 10 {
            let index = rand::thread_rng().gen::<u32>() % bit_count;
            set.insert(index);
        }
        for i in &set {
            bitmap.set(*i);
        }
        assert_eq!(bitmap.num_of_valid_bits(), set.len() as u32);
        for i in set {
            assert!(bitmap.get(i));
        }
    }
}
