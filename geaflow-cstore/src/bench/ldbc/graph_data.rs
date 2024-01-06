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

use std::{fmt, fmt::Formatter, rc::Rc};

pub struct LdbcVertex {
    pub id: u64,
    pub label: Rc<String>,
    pub ts: u64,
    pub property: Rc<Vec<u8>>,
}

impl fmt::Display for LdbcVertex {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "v:{}-{}-{}", self.id, self.label, self.ts)
    }
}

pub struct LdbcEdge {
    pub sid: u64,
    pub label: Rc<String>,
    pub ts: u64,
    pub direct: Direct,
    pub tid: u64,
    pub property: Rc<Vec<u8>>,
}

impl fmt::Display for LdbcEdge {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "e:{}-{}-{}-{}-{}",
            self.sid, self.label, self.direct, self.ts, self.tid
        )
    }
}

pub enum Direct {
    Out,
    In,
}

impl fmt::Display for Direct {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Direct::Out => write!(f, "out"),
            Direct::In => write!(f, "in"),
        }
    }
}

pub enum GraphStruct {
    Vertex(LdbcVertex),
    Edge(LdbcEdge),
    BothEdge(LdbcEdge, LdbcEdge),
    None,
}
