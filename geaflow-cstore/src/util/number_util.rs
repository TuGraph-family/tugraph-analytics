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

#![allow(dead_code)]

use std::str::FromStr;

use num::Integer;

const UNIT: u64 = 1;
pub const B: u64 = UNIT;
pub const KIB: u64 = UNIT * 1024;
pub const MIB: u64 = KIB * KIB;
pub const GIB: u64 = MIB * KIB;
pub const TIB: u64 = GIB * KIB;
pub const PIB: u64 = TIB * KIB;

#[macro_export]
macro_rules! convert_two_u32_to_u64 {
    ($v1:expr, $v2:expr) => {
        (($v1 as u64) << 32) | ($v2 as u64 & 0xffffffff)
    };
}

#[macro_export]
macro_rules! convert_u64_to_u32 {
    ($val:expr) => {{
        let v = $val;
        assert_eq!(v, v as u32 as u64);
        v as u32
    }};
}

#[macro_export]
macro_rules! convert_u64_to_i32 {
    ($val:expr) => {{
        let v = $val;
        assert_eq!(v, v as i32 as u64);
        v as i32
    }};
}

#[macro_export]
macro_rules! convert_usize_to_u32 {
    ($val:expr) => {{
        let v = $val;
        assert_eq!(v, v as u32 as usize);
        v as u32
    }};
}

#[macro_export]
macro_rules! convert_u64_to_usize {
    ($val:expr) => {{
        let v = $val;
        assert_eq!(v, v as usize as u64);
        v as usize
    }};
}

#[macro_export]
macro_rules! convert_usize_to_u16 {
    ($val:expr) => {{
        let v = $val;
        assert_eq!(v, v as u16 as usize);
        v as u16
    }};
}

#[macro_export]
macro_rules! get_u8_from_bytes {
    ($data_buffer:expr, $cursor:expr) => {{
        let cursor = $cursor;
        u8::from_be_bytes(
            $data_buffer[$cursor..cursor + $crate::SIZE_OF_U8]
                .try_into()
                .unwrap(),
        )
    }};
}

#[macro_export]
macro_rules! get_u16_from_bytes {
    ($data_buffer:expr, $cursor:expr) => {{
        let cursor = $cursor;
        u16::from_be_bytes(
            $data_buffer[$cursor..cursor + $crate::SIZE_OF_U16]
                .try_into()
                .unwrap(),
        )
    }};
}

#[macro_export]
macro_rules! get_u32_from_bytes {
    ($data_buffer:expr, $cursor:expr) => {{
        let cursor = $cursor;
        u32::from_be_bytes(
            $data_buffer[$cursor..cursor + $crate::SIZE_OF_U32]
                .try_into()
                .unwrap(),
        )
    }};
}

#[macro_export]
macro_rules! get_u64_from_bytes {
    ($data_buffer:expr, $start_offset:expr) => {{
        let start_offset = $start_offset;
        u64::from_be_bytes(
            $data_buffer[$start_offset..start_offset + $crate::SIZE_OF_U64]
                .try_into()
                .unwrap(),
        )
    }};
}

#[macro_export]
macro_rules! get_usize_from_bytes {
    ($data_buffer:expr, $start_offset:expr) => {{
        let start_offset = $start_offset;
        usize::from_be_bytes(
            $data_buffer[$start_offset..start_offset + $crate::SIZE_OF_USIZE]
                .try_into()
                .unwrap(),
        )
    }};
}

pub fn get_integer_from_str_suffix<T>(ori: &str, delimeter: &str) -> Option<T>
where
    T: FromStr + Integer,
{
    let pos = ori.rfind(delimeter)?;
    let suffix = &ori[pos + 1..];
    suffix.parse().ok()
}

pub fn get_integer_from_str_prefix<T>(ori: &str, delimeter: &str) -> Option<T>
where
    T: FromStr + Integer,
{
    let pos = ori.find(delimeter)?;
    let suffix = &ori[0..pos];
    suffix.parse().ok()
}
