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

use crate::{CHECK_POINT_SUFFIX, DIR_NAME_SPLICER, FILE_PATH_DELIMITER};

pub mod id_dict;
pub mod label_dict;
pub mod ttl_dict;

pub struct DictUtil {}

impl DictUtil {
    // path rule:
    // /store_namespace\/job_name/store_name/shard/dict_name/dict_version/xxx.sst
    pub fn get_snapshot_path(name_space: &str, dict_key: &str, version: u64) -> String {
        let mut new_path = String::new();
        new_path.push_str(name_space);
        new_path.push_str(FILE_PATH_DELIMITER);
        new_path.push_str(dict_key);
        new_path.push_str(DIR_NAME_SPLICER);
        new_path.push_str(CHECK_POINT_SUFFIX);
        new_path.push_str(FILE_PATH_DELIMITER);
        new_path.push_str(version.to_string().as_str());

        new_path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dict_get_snapshot_path() {
        let snapshot_path = DictUtil::get_snapshot_path("/rayag/test", "dict", 1);

        assert_eq!(snapshot_path, "/rayag/test/dict_chk/1");
    }
}
