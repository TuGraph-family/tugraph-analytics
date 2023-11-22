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
    fs,
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
    rc::Rc,
    str::FromStr,
};

use flate2::read::GzDecoder;
use rustc_hash::FxHashMap;

use super::{
    EdgeParser, GraphStruct, HashFunc, Parser, VertexParser, DYNAMIC_LABELS, STATIC_LABELS,
};
use crate::util::iterator::chain_iterator::ChainIterator;

type DParser = Rc<dyn Parser>;

#[allow(dead_code)]
pub struct LdbcSource {
    is_local: bool,
    basic_path: String,
    config: FxHashMap<String, String>,
}

impl LdbcSource {
    pub fn new(conf: &FxHashMap<String, String>) -> Self {
        let local_ldbc = conf
            .get("local.ldbc.path")
            .expect("local ldbc path not set");

        LdbcSource {
            is_local: local_ldbc.is_empty(),
            basic_path: format!("{}{}", local_ldbc, "/initial_snapshot/"),
            config: conf.clone(),
        }
    }

    pub fn get_iter(&self) -> ChainIterator<FileReader> {
        let mut vec = Vec::with_capacity(STATIC_LABELS.len() + DYNAMIC_LABELS.len());
        for label in STATIC_LABELS {
            vec.append(&mut self.iter_vec(label.to_string()));
        }
        for label in DYNAMIC_LABELS {
            vec.append(&mut self.iter_vec(label.to_string()));
        }
        ChainIterator::new(vec)
    }

    fn iter_vec(&self, label: String) -> Vec<Option<FileReader>> {
        let ldbc_type = if DYNAMIC_LABELS.contains(&label.as_str()) {
            "/dynamic/"
        } else {
            "/static/"
        };
        let is_edge = label.contains('_');
        let local_path = format!("{}{}{}", self.basic_path, ldbc_type, label);
        let paths =
            fs::read_dir(&local_path).unwrap_or_else(|_| panic!("dir not found {}", &local_path));
        let label_rc = Rc::new(label);
        let is_static = LdbcSource::is_static(&label_rc);

        let parser: DParser = if is_edge {
            let parts = label_rc.split('_').collect::<Vec<&str>>();
            let edge_label_rc = Rc::new(String::from(parts[1]));
            Rc::new(EdgeParser::new(
                Rc::clone(&edge_label_rc),
                is_static,
                true,
                HashFunc::from_str(parts[0]).unwrap(),
                HashFunc::from_str(parts[2]).unwrap(),
            ))
        } else {
            Rc::new(VertexParser::new(
                Rc::clone(&label_rc),
                is_static,
                HashFunc::from_str(label_rc.as_str()).unwrap(),
            ))
        };

        let mut it_vec = vec![];
        for path in paths {
            let file_name = path.as_ref().unwrap().file_name();
            let file_name_str = file_name.to_str().unwrap();
            if file_name_str.ends_with("crc") || file_name_str.eq("_SUCCESS") {
                continue;
            }
            let path_buf = path.as_ref().unwrap().path();
            let path_ref = path_buf.as_path();

            it_vec.push(Some(FileReader::new(path_ref, Rc::clone(&parser))));
        }

        it_vec
    }

    fn is_static(label: &Rc<String>) -> bool {
        STATIC_LABELS.contains(&label.as_str())
    }
}

pub struct FileReader {
    pub iter: Box<dyn Iterator<Item = GraphStruct>>,
}

impl FileReader {
    fn new(path: &Path, parser: DParser) -> Self {
        let it: Box<dyn Iterator<Item = GraphStruct>> =
            if path.file_name().unwrap().to_str().unwrap().ends_with("gz") {
                let reader = BufReader::new(GzDecoder::new(File::open(path).unwrap()));
                let mut a = reader.lines();
                a.next();
                Box::new(a.map(move |line| parser.parse(&line.unwrap())))
            } else {
                let reader = BufReader::new(File::open(path).unwrap());
                let mut a = reader.lines();
                a.next();
                Box::new(a.map(move |line| parser.parse(&line.unwrap())))
            };

        FileReader { iter: it }
    }
}

impl Iterator for FileReader {
    type Item = GraphStruct;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

#[cfg(test)]
mod tests {
    use rustc_hash::FxHashMap;

    use crate::{
        bench::ldbc::ldbc_source::LdbcSource,
        log_util::{info, LogLevel, LogType},
        util::log_util,
        GraphStruct,
    };

    #[ignore = "Timing task execute it"]
    #[test]
    pub fn test_ldbc() {
        log_util::try_init(LogType::ConsoleAndFile, LogLevel::Debug, 0);

        let mut config = FxHashMap::default();
        config.insert(String::from("local.ldbc.path"), String::from("/tmp/sf0.1"));
        let graph_builder = LdbcSource::new(&config);
        let all_data = graph_builder.get_iter();
        let mut vcount = 0;
        let mut ecount = 0;
        for record in all_data {
            match record {
                GraphStruct::Edge(_edge) => ecount += 1,
                GraphStruct::BothEdge(_edge1, _edge2) => ecount += 2,
                GraphStruct::Vertex(_vertex) => vcount += 1,
                _ => {}
            }
        }
        info!("vertex num {}, edge num {}", vcount, ecount);
    }
}
