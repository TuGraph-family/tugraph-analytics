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

use std::rc::Rc;

use chrono::DateTime;

use super::{Direct, GraphStruct, LdbcEdge, LdbcVertex};
use crate::bench::ldbc::HashFunc;

pub trait Parser {
    fn parse_static(&self, line: &str) -> GraphStruct;
    fn parse_dynamic(&self, line: &str) -> GraphStruct;
    fn parse(&self, line: &str) -> GraphStruct;
}

pub struct VertexParser {
    label: Rc<String>,
    is_static: bool,
    f: HashFunc,
}

impl VertexParser {
    pub fn new(label: Rc<String>, is_static: bool, f: HashFunc) -> Self {
        VertexParser {
            label: Rc::clone(&label),
            is_static,
            f,
        }
    }
}

impl Parser for VertexParser {
    fn parse_static(&self, line: &str) -> GraphStruct {
        let parts = line.split('|').collect::<Vec<&str>>();
        let origin_id: u64 = parts[0].parse().unwrap();
        let id = self.f.calc(origin_id);
        let prop = Rc::new(Vec::from(line.as_bytes()));
        GraphStruct::Vertex(LdbcVertex {
            id,
            label: Rc::clone(&self.label),
            ts: 0,
            property: Rc::clone(&prop),
        })
    }

    fn parse_dynamic(&self, line: &str) -> GraphStruct {
        let parts = line.split('|').collect::<Vec<&str>>();
        let ts = DateTime::parse_from_rfc3339(parts[0])
            .unwrap()
            .timestamp_millis() as u64;
        let origin_id: u64 = parts[1].parse().unwrap();
        let id = self.f.calc(origin_id);
        let prop = Rc::new(Vec::from(line.as_bytes()));
        GraphStruct::Vertex(LdbcVertex {
            id,
            label: Rc::clone(&self.label),
            ts,
            property: Rc::clone(&prop),
        })
    }

    fn parse(&self, line: &str) -> GraphStruct {
        if self.is_static {
            self.parse_static(line)
        } else {
            self.parse_dynamic(line)
        }
    }
}

#[allow(dead_code)]
pub struct EdgeParser {
    label: Rc<String>,
    is_static: bool,
    is_both: bool,
    f1: HashFunc,
    f2: HashFunc,
}

impl EdgeParser {
    pub fn new(
        label: Rc<String>,
        is_static: bool,
        is_both: bool,
        f1: HashFunc,
        f2: HashFunc,
    ) -> Self {
        EdgeParser {
            label: Rc::clone(&label),
            is_static,
            is_both,
            f1,
            f2,
        }
    }
}

impl Parser for EdgeParser {
    fn parse_static(&self, line: &str) -> GraphStruct {
        let parts = line.split('|').collect::<Vec<&str>>();
        let sid = self.f1.calc(parts[0].parse().unwrap());
        let tid = self.f2.calc(parts[1].parse().unwrap());
        let prop = Rc::new(Vec::from(line));
        GraphStruct::BothEdge(
            LdbcEdge {
                sid,
                label: Rc::clone(&self.label),
                ts: 0,
                direct: Direct::Out,
                tid,
                property: Rc::clone(&prop),
            },
            LdbcEdge {
                tid,
                label: Rc::clone(&self.label),
                ts: 0,
                direct: Direct::In,
                sid,
                property: Rc::clone(&prop),
            },
        )
    }

    fn parse_dynamic(&self, line: &str) -> GraphStruct {
        let parts = line.split('|').collect::<Vec<&str>>();
        let ts = DateTime::parse_from_rfc3339(parts[0])
            .unwrap()
            .timestamp_millis() as u64;
        let sid = self.f1.calc(parts[1].parse().unwrap());
        let tid = self.f2.calc(parts[2].parse().unwrap());
        let prop = Rc::new(Vec::from(line));
        GraphStruct::BothEdge(
            LdbcEdge {
                sid,
                label: Rc::clone(&self.label),
                ts,
                direct: Direct::Out,
                tid,
                property: Rc::clone(&prop),
            },
            LdbcEdge {
                tid,
                label: Rc::clone(&self.label),
                ts,
                direct: Direct::In,
                sid,
                property: Rc::clone(&prop),
            },
        )
    }

    fn parse(&self, line: &str) -> GraphStruct {
        if self.is_static {
            self.parse_static(line)
        } else {
            self.parse_dynamic(line)
        }
    }
}
