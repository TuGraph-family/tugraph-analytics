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

use num_enum::{IntoPrimitive, TryFromPrimitive};
use strum_macros::{Display, EnumString};

/// Set of the compress type.
#[derive(Clone, Copy, IntoPrimitive, TryFromPrimitive, EnumString, Display, PartialEq, Debug)]
#[repr(u32)]
#[strum(ascii_case_insensitive)]
pub enum CompressType {
    Lz4 = 1,

    Snappy,

    Lz4Flex,

    None,
}

pub enum CompressHandler {
    Lz4,

    Snappy { encoder: Box<snap::raw::Encoder> },

    Lz4Flex,

    None,
}

impl CompressHandler {
    pub fn new(compress_type: CompressType) -> Self {
        match compress_type {
            CompressType::None => CompressHandler::None,
            CompressType::Snappy => CompressHandler::Snappy {
                encoder: (Box::new(snap::raw::Encoder::new())),
            },
            CompressType::Lz4 => CompressHandler::Lz4,
            CompressType::Lz4Flex => CompressHandler::Lz4Flex,
        }
    }
}

pub enum DecompressHandler {
    None,
    Snappy { decoder: Box<snap::raw::Decoder> },
    Lz4,
    Lz4Flex,
}

impl DecompressHandler {
    pub fn new(compress_type: CompressType) -> Self {
        match compress_type {
            CompressType::None => DecompressHandler::None,
            CompressType::Snappy => DecompressHandler::Snappy {
                decoder: (Box::new(snap::raw::Decoder::new())),
            },
            CompressType::Lz4 => DecompressHandler::Lz4,
            CompressType::Lz4Flex => DecompressHandler::Lz4Flex,
        }
    }
}

/// Decompress the data and access it into the buffer.
pub fn decompress(
    decompress_handler: &mut DecompressHandler,
    raw_buffer: &mut Vec<u8>,
    compressed_buffer: &[u8],
    compressed_len: u32,
) -> usize {
    // decompress according to compression type
    match decompress_handler {
        DecompressHandler::None => {
            raw_buffer.clear();
            raw_buffer.append(&mut compressed_buffer.to_owned());
            raw_buffer.len()
        }

        DecompressHandler::Snappy { decoder } => {
            decoder.decompress(compressed_buffer, raw_buffer).unwrap()
        }

        DecompressHandler::Lz4 => lz4::block::decompress_to_buffer(
            &compressed_buffer[..compressed_len as usize],
            None,
            raw_buffer,
        )
        .unwrap(),

        DecompressHandler::Lz4Flex => lz4_flex::block::decompress_into(
            &compressed_buffer[..compressed_len as usize],
            raw_buffer,
        )
        .unwrap(),
    }
}

/// Compress.
pub fn compress(
    compress_handler: &mut CompressHandler,
    raw_buffer: &[u8],
    compressed_buffer: &mut Vec<u8>,
) -> usize {
    // decompress according to compression type
    match compress_handler {
        CompressHandler::None => {
            compressed_buffer.clear();
            compressed_buffer.append(&mut raw_buffer.to_owned());
            compressed_buffer.len()
        }

        CompressHandler::Snappy { encoder } => {
            encoder.compress(raw_buffer, compressed_buffer).unwrap()
        }

        CompressHandler::Lz4 => lz4::block::compress_to_buffer(
            raw_buffer,
            lz4::block::CompressionMode::FAST(16).into(),
            true,
            compressed_buffer,
        )
        .unwrap(),

        CompressHandler::Lz4Flex => {
            lz4_flex::block::compress_into(raw_buffer, compressed_buffer).unwrap()
        }
    }
}
