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

use std::mem::swap;

use murmurhash3::murmurhash3_x86_32;

const C1: u32 = 0xcc9e2d51;
const C2: u32 = 0x1b873593;
const SEED: u32 = 48221234;

pub fn murmur_hash(str: &[u8]) -> u32 {
    murmurhash3_x86_32(str, SEED)
}

pub fn djb2_hash(str: &[u8]) -> u32 {
    let mut hash: u32 = 5381;
    for i in str {
        hash = ((hash << 5).wrapping_add(hash)).wrapping_add(*i as u32);
    }
    hash
}

fn rotate32(val: u32, shift: i32) -> u32 {
    if shift == 0 {
        val
    } else {
        (val >> shift) | (val << (32 - shift))
    }
}

fn mur(mut a: u32, mut h: u32) -> u32 {
    a = a.wrapping_mul(C1);
    a = rotate32(a, 17);
    a = a.wrapping_mul(C2);
    h ^= a;
    h = rotate32(h, 19);
    h.wrapping_mul(5).wrapping_add(0xe6546b64)
}

fn fmix(mut h: u32) -> u32 {
    h = h ^ (h >> 16);
    h = h.wrapping_mul(0x85ebca6b);
    h = h ^ (h >> 13);
    h = h.wrapping_mul(0xc2b2ae35);
    h = h ^ (h >> 16);
    h
}

fn fetch32(str: &[u8], index: u32) -> u32 {
    u32::from_le_bytes(str[index as usize..index as usize + 4].try_into().unwrap())
}

fn hash32_len0to4(str: &[u8], len: u32) -> u32 {
    let mut b: u32 = 0;
    let mut c: u32 = 9;
    for i in str {
        let v: i8 = *i as i8;
        if v >= 0 {
            b = b.wrapping_mul(C1).wrapping_add(v.unsigned_abs() as u32);
        } else {
            b = b.wrapping_mul(C1) - v.unsigned_abs() as u32;
        };

        c ^= b;
    }
    fmix(mur(b, mur(len, c)))
}

fn hash32_len5to12(str: &[u8], len: u32) -> u32 {
    let mut a = len;
    let mut b = len.wrapping_mul(5);
    let mut c = 9;
    let d = b;

    a += fetch32(str, 0);
    b += fetch32(str, len - 4);
    c += fetch32(str, (len >> 1) & 4);
    fmix(mur(c, mur(b, mur(a, d))))
}

fn hash32_len13to24(str: &[u8], len: u32) -> u32 {
    let a = fetch32(str, (len >> 1) - 4);
    let b = fetch32(str, 4);
    let c = fetch32(str, len - 8);
    let d = fetch32(str, len >> 1);
    let e = fetch32(str, 0);
    let f = fetch32(str, len - 4);
    let h = len;
    fmix(mur(f, mur(e, mur(d, mur(c, mur(b, mur(a, h)))))))
}

pub fn city_hash32(str: &[u8]) -> u32 {
    let len = str.len() as u32;
    if len <= 24 {
        if len > 12 {
            return hash32_len13to24(str, len);
        };
        if len > 4 {
            return hash32_len5to12(str, len);
        };
        return hash32_len0to4(str, len);
    };

    let mut h = len;
    let mut g = len.wrapping_mul(C1);
    let mut f = g;
    let mut a0 = rotate32(fetch32(str, len - 4).wrapping_mul(C1), 17).wrapping_mul(C2);
    let mut a1 = rotate32(fetch32(str, len - 8).wrapping_mul(C1), 17).wrapping_mul(C2);
    let mut a2 = rotate32(fetch32(str, len - 16).wrapping_mul(C1), 17).wrapping_mul(C2);
    let mut a3 = rotate32(fetch32(str, len - 12).wrapping_mul(C1), 17).wrapping_mul(C2);
    let mut a4 = rotate32(fetch32(str, len - 20).wrapping_mul(C1), 17).wrapping_mul(C2);
    h ^= a0;
    h = rotate32(h, 19);
    h = h.wrapping_mul(5).wrapping_add(0xe6546b64);
    h ^= a2;
    h = rotate32(h, 19);
    h = h.wrapping_mul(5).wrapping_add(0xe6546b64);
    g ^= a1;
    g = rotate32(g, 19);
    g = g.wrapping_mul(5).wrapping_add(0xe6546b64);
    g ^= a3;
    g = rotate32(g, 19);
    g = g.wrapping_mul(5).wrapping_add(0xe6546b64);
    f = f.wrapping_add(a4);
    f = rotate32(f, 19);
    f = f.wrapping_mul(5).wrapping_add(0xe6546b64);
    let mut iters = (len - 1) / 20;

    let mut offset = 0;
    loop {
        a0 = rotate32(fetch32(str, offset).wrapping_mul(C1), 17).wrapping_mul(C2);
        a1 = fetch32(str, offset + 4);
        a2 = rotate32(fetch32(str, offset + 8).wrapping_mul(C1), 17).wrapping_mul(C2);
        a3 = rotate32(fetch32(str, offset + 12).wrapping_mul(C1), 17).wrapping_mul(C2);
        a4 = fetch32(str, offset + 16);
        h ^= a0;
        h = rotate32(h, 18);
        h = h.wrapping_mul(5).wrapping_add(0xe6546b64);
        f = f.wrapping_add(a1);
        f = rotate32(f, 19);
        f = f.wrapping_mul(C1);
        g = g.wrapping_add(a2);
        g = rotate32(g, 18);
        g = g.wrapping_mul(5).wrapping_add(0xe6546b64);
        h ^= a3.wrapping_add(a1);
        h = rotate32(h, 19);
        h = h.wrapping_mul(5).wrapping_add(0xe6546b64);
        g ^= a4;
        g = g.to_be().wrapping_mul(5);
        h = h.wrapping_add(a4.wrapping_mul(5));
        h = h.to_be();
        f = f.wrapping_add(a0);
        swap(&mut f, &mut h);
        swap(&mut f, &mut g);
        offset += 20;
        iters -= 1;
        if iters == 0 {
            break;
        };
    }
    g = rotate32(g, 11).wrapping_mul(C1);
    g = rotate32(g, 17).wrapping_mul(C1);
    f = rotate32(f, 11).wrapping_mul(C1);
    f = rotate32(f, 17).wrapping_mul(C1);
    h = rotate32(h.wrapping_add(g), 19);
    h = h.wrapping_mul(5).wrapping_add(0xe6546b64);
    h = rotate32(h, 17).wrapping_mul(C1);
    h = rotate32(h.wrapping_add(f), 19);
    h = h.wrapping_mul(5).wrapping_add(0xe6546b64);
    h = rotate32(h, 17).wrapping_mul(C1);
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_city() {
        let source: Vec<u8> = Vec::from("abcd1234");
        assert_eq!(2377107069, city_hash32(&source));
    }
}
