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

pub use graph_data::{Direct, GraphStruct, LdbcEdge, LdbcVertex};
pub use parser::{EdgeParser, Parser, VertexParser};
use strum_macros::{Display, EnumString};

pub mod graph_data;
pub mod ldbc_source;
mod parser;

pub const DYNAMIC_LABELS: [&str; 23] = [
    "Comment",
    "Comment_hasCreator_Person",
    "Comment_hasTag_Tag",
    "Comment_isLocatedIn_Country",
    "Comment_replyOf_Comment",
    "Comment_replyOf_Post",
    "Forum",
    "Forum_containerOf_Post",
    "Forum_hasMember_Person",
    "Forum_hasModerator_Person",
    "Forum_hasTag_Tag",
    "Person",
    "Person_hasInterest_Tag",
    "Person_isLocatedIn_City",
    "Person_knows_Person",
    "Person_likes_Comment",
    "Person_likes_Post",
    "Person_studyAt_University",
    "Person_workAt_Company",
    "Post",
    "Post_hasCreator_Person",
    "Post_hasTag_Tag",
    "Post_isLocatedIn_Country",
];

pub const STATIC_LABELS: [&str; 8] = [
    "Organisation",
    "Organisation_isLocatedIn_Place",
    "Place",
    "Place_isPartOf_Place",
    "Tag",
    "TagClass",
    "TagClass_isSubclassOf_TagClass",
    "Tag_hasType_TagClass",
];

#[derive(Clone, EnumString, Display)]
#[strum(ascii_case_insensitive)]
pub enum HashFunc {
    Tag,
    Place,
    Country,
    City,
    Organisation,
    University,
    Company,
    TagClass,
    Comment,
    Forum,
    Person,
    Post,
}

impl HashFunc {
    pub fn calc(&self, x: u64) -> u64 {
        match *self {
            HashFunc::Tag => x * 10 + 1,
            HashFunc::Place => x * 10 + 4,
            HashFunc::Country => x * 10 + 4,
            HashFunc::City => x * 10 + 4,
            HashFunc::Organisation => x * 10 + 5,
            HashFunc::University => x * 10 + 5,
            HashFunc::Company => x * 10 + 5,
            HashFunc::TagClass => x * 10 + 7,
            HashFunc::Comment => x * 10 + 3,
            HashFunc::Forum => x * 10 + 1,
            HashFunc::Person => x * 10,
            HashFunc::Post => x * 10 + 2,
        }
    }
}
