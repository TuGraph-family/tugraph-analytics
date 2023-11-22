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

// The macros in this file are used to help generate generic functions.

/// with_bound, get_bound, set_bound
///
///  E.g.
///
/// #[derive(Debug, Default)]
/// struct Test {
///     pub a: u32,
///     pub b: String,
///     pub vec: Vec<Vec<(usize, usize)>>,
/// }
///
/// with_bound!(Test, self, a: u32, b: String, vec: Vec<Vec<(usize, usize)>>);
/// get_bound!(Test, self, a: u32, b: String, vec: Vec<Vec<(usize, usize)>>);
/// set_bound!(Test, self, a: u32, b: String, vec: Vec<Vec<(usize, usize)>>);
///
/// let a = Test::default().with_a(3).with_b("a".to_string()).with_vec(vec![vec!
/// [(1,2)]]);

#[macro_export]
macro_rules! with_bound {
    ($struct_name:ident, $self:ident, $($field:ident: $type:ty),*) => {
            impl $struct_name {
                $(
                    ::paste::paste! {
                        pub fn [<with_ $field>] (mut $self, val:$type) -> Self {
                            $self.$field = val;
                            $self
                        }
                    }
                )*
            }
    };
}

#[macro_export]
macro_rules! get_bound {
    ($struct_name:ident, $self:ident, $($field:ident: $type:ty),*) => {
        impl $struct_name {
            $(
                ::paste::paste! {
                    pub fn [<get_ $field>] (&$self) -> &$type {
                        &$self.$field
                    }
                }
            )*
        }
    };
}

#[macro_export]
macro_rules! set_bound {
    ($struct_name:ident, $self:ident, $($field:ident: $type:ty),*) => {
        impl $struct_name {
            $(
                ::paste::paste! {
                    pub fn [<set_ $field>] (&mut $self, val:$type) {
                        $self.$field = val;
                    }
                }
            )*
        }
    };
}
