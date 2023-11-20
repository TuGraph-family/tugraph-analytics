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

use crate::FILE_PATH_DELIMITER;

#[inline]
pub fn get_rfind_front_section<'a>(source: &'a str, delimiter: &str) -> &'a str {
    if let Some(pos) = source.rfind(delimiter) {
        &source[..pos]
    } else {
        source
    }
}

#[inline]
pub fn get_rfind_back_section<'a>(source: &'a str, delimiter: &str) -> &'a str {
    if let Some(pos) = source.rfind(delimiter) {
        &source[pos + delimiter.len()..]
    } else {
        source
    }
}

#[inline]
pub fn concat_str(first: &str, second: &str) -> String {
    let mut full_str = String::from(first);
    full_str.push_str(second);
    full_str
}

#[inline]
pub fn normalize_dir_path(dir_path: &str) -> String {
    if !dir_path.ends_with(FILE_PATH_DELIMITER) {
        concat_str(dir_path, FILE_PATH_DELIMITER)
    } else {
        dir_path.to_string()
    }
}

#[inline]
pub fn get_rel_path(root: &str, path: &str) -> String {
    debug_assert!(root != path, "get rel path with root is invalid");

    path[root.len()..].to_string()
}
