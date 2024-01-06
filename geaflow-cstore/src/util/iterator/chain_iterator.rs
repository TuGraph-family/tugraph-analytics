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

pub struct ChainIterator<A: Iterator> {
    vec: Vec<Option<A>>,
    index: usize,
}

impl<A: Iterator> ChainIterator<A> {
    pub fn new(vec: Vec<Option<A>>) -> ChainIterator<A> {
        ChainIterator { vec, index: 0 }
    }
}

impl<A: Iterator> Iterator for ChainIterator<A> {
    type Item = A::Item;

    fn next(&mut self) -> Option<A::Item> {
        and_then_or_close(&mut self.vec[self.index], Iterator::next).or_else(|| {
            self.index += 1;
            while self.index < self.vec.len() {
                let x = Iterator::next(self.vec[self.index].as_mut()?);
                if x.is_none() {
                    self.index += 1
                } else {
                    return x;
                }
            }
            None
        })
    }
}

#[inline]
fn and_then_or_close<T, U>(opt: &mut Option<T>, f: impl FnOnce(&mut T) -> Option<U>) -> Option<U> {
    let x = f(opt.as_mut()?);
    if x.is_none() {
        *opt = None;
    }
    x
}

#[cfg(test)]
mod tests {
    use crate::util::iterator::chain_iterator::ChainIterator;

    #[test]
    pub fn test_chain_iterator() {
        let a = vec![1, 2, 3];
        let b = vec![];
        let c = vec![4, 5, 6];
        let d = vec![7, 8, 9];

        let vv = vec![
            Some(a.into_iter()),
            Some(b.into_iter()),
            Some(c.into_iter()),
            Some(d.into_iter()),
        ];
        let chain = ChainIterator::new(vv);
        let mut count = 0;
        for _v in chain {
            count += 1;
        }
        assert_eq!(count, 9)
    }
}
