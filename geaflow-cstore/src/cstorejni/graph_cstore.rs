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

use std::sync::OnceLock;

use jni::{
    objects::{
        AsJArrayRaw, GlobalRef, JByteArray, JClass, JMap, JMethodID, JObject, JObjectArray, JString,
    },
    sys::{jboolean, jint, jlong, jlongArray, JNI_TRUE},
    JNIEnv,
};
use rustc_hash::FxHashMap;

use crate::{
    api::graph::{edge::Edge, vertex::Vertex, EdgeDirection},
    log_util::{self, info, LogLevel, LogType},
    CStore, CStoreConfigBuilder, ConfigMap,
};

pub static VERTEX_CLAZZ: OnceLock<(GlobalRef, JMethodID)> = OnceLock::new();
pub static EDGE_CLAZZ: OnceLock<(GlobalRef, JMethodID)> = OnceLock::new();
pub static VERTEX_AND_EDGE_CLAZZ: OnceLock<(GlobalRef, JMethodID)> = OnceLock::new();

#[no_mangle]
pub unsafe extern "system" fn Java_com_antgroup_geaflow_store_cstore_NativeGraphStore_construct(
    mut env: JNIEnv,
    _: JClass,
    name: JString,
    shard: jint,
    map: JObject,
) -> jlong {
    log_util::try_init(LogType::ConsoleAndFile, LogLevel::Info, shard as u32);
    init_java_class_once(&mut env);
    let store = intern_construct(&mut env, name, shard as u32, map);
    info!("construct rust cstore shard {} success", shard);
    Box::into_raw(Box::new(store)) as jlong
}

unsafe fn intern_construct(env: &mut JNIEnv, name: JString, shard: u32, map: JObject) -> CStore {
    let _name: String = env.get_string_unchecked(&name).unwrap().into();
    let mut map: FxHashMap<String, String> = jmap_to_hashmap(env, &map).unwrap();
    map.insert(String::from("store.shard_index"), shard.to_string());
    let config_map = ConfigMap::from_iter(map.iter());

    let config = CStoreConfigBuilder::default()
        .set_with_map(&config_map)
        .build();

    CStore::new(config)
}

fn init_java_class_once(env: &mut JNIEnv) {
    VERTEX_CLAZZ.get_or_init(|| {
        let jclazz = env
            .find_class("com/antgroup/geaflow/store/cstore/VertexContainer")
            .expect("class not found");
        let method_id = env.get_method_id(jclazz, "<init>", "([B)V").unwrap();
        let jclazz = env
            .find_class("com/antgroup/geaflow/store/cstore/VertexContainer")
            .expect("class not found");
        let java_vertex_clazz = env.new_global_ref(jclazz).unwrap();
        (java_vertex_clazz, method_id)
    });

    EDGE_CLAZZ.get_or_init(|| {
        let jclazz = env
            .find_class("com/antgroup/geaflow/store/cstore/EdgeContainer")
            .expect("class not found");
        let method_id = env.get_method_id(jclazz, "<init>", "([B)V").unwrap();
        let jclazz = env
            .find_class("com/antgroup/geaflow/store/cstore/EdgeContainer")
            .expect("class not found");
        let java_edge_clazz = env.new_global_ref(jclazz).unwrap();
        (java_edge_clazz, method_id)
    });

    VERTEX_AND_EDGE_CLAZZ.get_or_init(|| {
        let jclazz = env
            .find_class("com/antgroup/geaflow/store/cstore/VertexAndEdgeContainer")
            .expect("class not found");
        let method_id = env.get_method_id(jclazz, "<init>", "([BJJ)V").unwrap();
        let jclazz = env
            .find_class("com/antgroup/geaflow/store/cstore/VertexAndEdgeContainer")
            .expect("class not found");
        let java_vertex_and_edge_clazz = env.new_global_ref(jclazz).unwrap();
        (java_vertex_and_edge_clazz, method_id)
    });
}

/// # Safety
///
/// This function should not be called before the Operator are ready.
#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "system" fn Java_com_antgroup_geaflow_store_cstore_NativeGraphStore_disposeInternal(
    _: JNIEnv,
    _: JClass,
    store: *mut CStore,
) {
    drop(Box::from_raw(store));
}

/// Store Actions: flush/close/compact/archive/recover.
#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "system" fn Java_com_antgroup_geaflow_store_cstore_NativeGraphStore_flush(
    _env: JNIEnv,
    _: JClass,
    store: *mut CStore,
) {
    let store = &mut *store;
    store.flush();
}

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "system" fn Java_com_antgroup_geaflow_store_cstore_NativeGraphStore_close(
    _env: JNIEnv,
    _: JClass,
    store: *mut CStore,
) {
    let store = &mut *store;
    store.close();
}

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "system" fn Java_com_antgroup_geaflow_store_cstore_NativeGraphStore_compact(
    _env: JNIEnv,
    _: JClass,
    store: *mut CStore,
) {
    let store = &mut *store;
    store.compact();
}

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "system" fn Java_com_antgroup_geaflow_store_cstore_NativeGraphStore_archive(
    _env: JNIEnv,
    _: JClass,
    store: *mut CStore,
    version: jlong,
) {
    let store = &mut *store;
    let version = version as u64;
    store.archive(version);
}

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "system" fn Java_com_antgroup_geaflow_store_cstore_NativeGraphStore_recover(
    _env: JNIEnv,
    _: JClass,
    store: *mut CStore,
    version: jlong,
) {
    let store = &mut *store;
    let version = version as u64;
    store.recover(version);
}

/// Vertex Operators.

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "system" fn Java_com_antgroup_geaflow_store_cstore_NativeGraphStore_addVertex(
    env: JNIEnv,
    _: JClass,
    store: *mut CStore,
    key: JByteArray,
    ts: jlong,
    label: JString,
    value: JByteArray,
) {
    let src_id = env.convert_byte_array(key).unwrap();
    let ts = ts as u64;

    let label: String = env.get_string_unchecked(&label).unwrap().into();
    let property = if value.is_null() {
        vec![]
    } else {
        env.convert_byte_array(value).unwrap()
    };
    let store = &mut *store;

    store.add_vertex(Vertex::create_id_time_label_vertex(
        src_id, ts, label, property,
    ));
}

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "system" fn Java_com_antgroup_geaflow_store_cstore_NativeGraphStore_getVertex(
    env: JNIEnv,
    _: JClass,
    store: *mut CStore,
    key: JByteArray,
) -> jlong {
    let store = &mut *store;
    let key = env.convert_byte_array(key).unwrap();

    let vertex_iter = store.get_serialized_vertex(&key);

    Box::into_raw(Box::new(vertex_iter)) as jlong
}

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "system" fn Java_com_antgroup_geaflow_store_cstore_NativeGraphStore_getVertexWithPushdown(
    env: JNIEnv,
    _: JClass,
    store: *mut CStore,
    key: JByteArray,
    pushdown: JByteArray,
) -> jlong {
    let store = &mut *store;
    let key = env.convert_byte_array(key).unwrap();
    let byte_pushdown = env.convert_byte_array(pushdown).unwrap();

    let vertex_iter = store.get_serialized_vertex_with_filter_pushdown_buf(&key, &byte_pushdown);
    Box::into_raw(Box::new(vertex_iter)) as jlong
}

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "system" fn Java_com_antgroup_geaflow_store_cstore_NativeGraphStore_scanVertex(
    _env: JNIEnv,
    _: JClass,
    store: *mut CStore,
) -> jlong {
    let store = &mut *store;
    let vertex_iter = store.scan_serialized_vertex();

    Box::into_raw(Box::new(vertex_iter)) as jlong
}

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "system" fn Java_com_antgroup_geaflow_store_cstore_NativeGraphStore_scanVertexWithPushdown(
    env: JNIEnv,
    _: JClass,
    store: *mut CStore,
    pushdown: JByteArray,
) -> jlong {
    let store = &mut *store;
    let byte_pushdown = env.convert_byte_array(pushdown).unwrap();
    let vertex_iter = store.scan_serialized_vertex_with_filter(&byte_pushdown);

    Box::into_raw(Box::new(vertex_iter)) as jlong
}

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "system" fn Java_com_antgroup_geaflow_store_cstore_NativeGraphStore_scanVertexWithKeys(
    mut env: JNIEnv,
    _: JClass,
    store: *mut CStore,
    keys: JObjectArray,
    key_num: jint,
) -> jlong {
    let store = &mut *store;
    let key_array = convert_byte_array(&mut env, &keys, key_num);
    let byte_pushdown = vec![];
    let vertex_iter =
        store.get_multi_serialized_vertex_with_filter_pushdown_buf(&key_array, &byte_pushdown);
    Box::into_raw(Box::new(vertex_iter)) as jlong
}

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "system" fn Java_com_antgroup_geaflow_store_cstore_NativeGraphStore_scanVertexWithKeysAndPushdown(
    mut env: JNIEnv,
    _: JClass,
    store: *mut CStore,
    keys: JObjectArray,
    key_num: jint,
    pushdown: JByteArray,
) -> jlong {
    let store = &mut *store;
    let key_array = convert_byte_array(&mut env, &keys, key_num);
    let byte_pushdown = env.convert_byte_array(pushdown).unwrap();

    let vertex_iter =
        store.get_multi_serialized_vertex_with_filter_pushdown_buf(&key_array, &byte_pushdown);
    Box::into_raw(Box::new(vertex_iter)) as jlong
}

/// Edge Operators.

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "system" fn Java_com_antgroup_geaflow_store_cstore_NativeGraphStore_addEdge(
    env: JNIEnv,
    _: JClass,
    store: *mut CStore,
    src: JByteArray,
    ts: jlong,
    label: JString,
    is_out: jboolean,
    target: JByteArray,
    value: JByteArray,
) {
    let src_id = env.convert_byte_array(src).unwrap();
    let ts = ts as u64;

    let label: String = env.get_string_unchecked(&label).unwrap().into();
    let direction = if is_out == JNI_TRUE {
        EdgeDirection::Out
    } else {
        EdgeDirection::In
    };
    let target_id = env.convert_byte_array(target).unwrap();
    let property = if value.is_null() {
        vec![]
    } else {
        env.convert_byte_array(value).unwrap()
    };
    let store = &mut *store;

    store.add_edge(Edge::create_id_time_label_edge(
        src_id, target_id, ts, label, direction, property,
    ));
}

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "system" fn Java_com_antgroup_geaflow_store_cstore_NativeGraphStore_getEdges(
    env: JNIEnv,
    _: JClass,
    store: *mut CStore,
    key: JByteArray,
) -> jlong {
    let store = &mut *store;
    let key = env.convert_byte_array(key).unwrap();
    let edge_iter = store.get_serialized_edge(&key);

    Box::into_raw(Box::new(edge_iter)) as jlong
}

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "system" fn Java_com_antgroup_geaflow_store_cstore_NativeGraphStore_getEdgesWithPushdown(
    env: JNIEnv,
    _: JClass,
    store: *mut CStore,
    key: JByteArray,
    pushdown: JByteArray,
) -> jlong {
    let store = &mut *store;
    let key = env.convert_byte_array(key).unwrap();
    let byte_pushdown = env.convert_byte_array(pushdown).unwrap();

    let edge_iter = store.get_serialized_edge_with_filter_pushdown_buf(&key, &byte_pushdown);
    Box::into_raw(Box::new(edge_iter)) as jlong
}

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "system" fn Java_com_antgroup_geaflow_store_cstore_NativeGraphStore_scanEdges(
    _env: JNIEnv,
    _: JClass,
    store: *mut CStore,
) -> jlong {
    let store = &mut *store;
    let edge_iter = store.scan_serialized_edge();

    Box::into_raw(Box::new(edge_iter)) as jlong
}

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "system" fn Java_com_antgroup_geaflow_store_cstore_NativeGraphStore_scanEdgesWithPushdown(
    env: JNIEnv,
    _: JClass,
    store: *mut CStore,
    pushdown: JByteArray,
) -> jlong {
    let store = &mut *store;
    let byte_pushdown = env.convert_byte_array(pushdown).unwrap();
    let edge_iter = store.scan_serialized_edge_with_filter(&byte_pushdown);

    Box::into_raw(Box::new(edge_iter)) as jlong
}

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "system" fn Java_com_antgroup_geaflow_store_cstore_NativeGraphStore_scanEdgesWithKeys(
    mut env: JNIEnv,
    _: JClass,
    store: *mut CStore,
    keys: JObjectArray,
    key_num: jint,
) -> jlong {
    let store = &mut *store;

    let key_array = convert_byte_array(&mut env, &keys, key_num);
    let byte_pushdown = vec![];

    let edge_iter =
        store.get_multi_serialized_edge_with_filter_pushdown_buf(&key_array, &byte_pushdown);
    Box::into_raw(Box::new(edge_iter)) as jlong
}

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "system" fn Java_com_antgroup_geaflow_store_cstore_NativeGraphStore_scanEdgesWithKeysAndPushdown(
    mut env: JNIEnv,
    _: JClass,
    store: *mut CStore,
    keys: JObjectArray,
    key_num: jint,
    pushdown: JByteArray,
) -> jlong {
    let store = &mut *store;

    let key_array = convert_byte_array(&mut env, &keys, key_num);
    let byte_pushdown = env.convert_byte_array(pushdown).unwrap();

    let edge_iter =
        store.get_multi_serialized_edge_with_filter_pushdown_buf(&key_array, &byte_pushdown);
    Box::into_raw(Box::new(edge_iter)) as jlong
}

/// VertexAndEdge Operators.
#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "system" fn Java_com_antgroup_geaflow_store_cstore_NativeGraphStore_getVertexAndEdge(
    env: JNIEnv,
    _: JClass,
    store: *mut CStore,
    key: JByteArray,
) -> jlongArray {
    let store = &mut *store;
    let key = env.convert_byte_array(key).unwrap();
    let ve = store.get_serialized_vertex_and_edge(&key);

    let a = env.new_long_array(2).unwrap();
    let array = [
        Box::into_raw(Box::new(ve.vertex_iter)) as jlong,
        Box::into_raw(Box::new(ve.edge_iter)) as jlong,
    ];
    env.set_long_array_region(&a, 0, &array).unwrap();
    a.as_jarray_raw()
}

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "system" fn Java_com_antgroup_geaflow_store_cstore_NativeGraphStore_getVertexAndEdgeWithPushdown(
    env: JNIEnv,
    _: JClass,
    store: *mut CStore,
    key: JByteArray,
    pushdown: JByteArray,
) -> jlongArray {
    let store = &mut *store;
    let key = env.convert_byte_array(key).unwrap();
    let pushdown = env.convert_byte_array(pushdown).unwrap();
    let ve = store.get_serialized_vertex_and_edge_with_filter_pushdown_buf(&key, &pushdown);

    let a = env.new_long_array(2).unwrap();
    let array = [
        Box::into_raw(Box::new(ve.vertex_iter)) as jlong,
        Box::into_raw(Box::new(ve.edge_iter)) as jlong,
    ];
    env.set_long_array_region(&a, 0, &array).unwrap();
    a.as_jarray_raw()
}

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "system" fn Java_com_antgroup_geaflow_store_cstore_NativeGraphStore_scanVertexAndEdge(
    _env: JNIEnv,
    _: JClass,
    store: *mut CStore,
) -> jlong {
    let store = &mut *store;
    let ve_iter = store.scan_serialized_vertex_and_edge();
    Box::into_raw(Box::new(ve_iter)) as jlong
}

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "system" fn Java_com_antgroup_geaflow_store_cstore_NativeGraphStore_scanVertexAndEdgeWithPushdown(
    env: JNIEnv,
    _: JClass,
    store: *mut CStore,
    pushdown: JByteArray,
) -> jlong {
    let store = &mut *store;
    let byte_pushdown = env.convert_byte_array(pushdown).unwrap();
    let ve_iter = store.scan_serialized_vertex_and_edge_with_filter(&byte_pushdown);
    Box::into_raw(Box::new(ve_iter)) as jlong
}

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "system" fn Java_com_antgroup_geaflow_store_cstore_NativeGraphStore_scanVertexAndEdgeWithKeys(
    mut env: JNIEnv,
    _: JClass,
    store: *mut CStore,
    keys: JObjectArray,
    key_num: jint,
) -> jlong {
    let store = &mut *store;
    let key_array = convert_byte_array(&mut env, &keys, key_num);
    let byte_pushdown = vec![];
    let ve_iter = store
        .get_multi_serialized_vertex_and_edge_with_filter_pushdown_buf(&key_array, &byte_pushdown);
    Box::into_raw(Box::new(ve_iter)) as jlong
}

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "system" fn Java_com_antgroup_geaflow_store_cstore_NativeGraphStore_scanVertexAndEdgeWithKeysAndPushdown(
    mut env: JNIEnv,
    _: JClass,
    store: *mut CStore,
    keys: JObjectArray,
    key_num: jint,
    pushdown: JByteArray,
) -> jlong {
    let store = &mut *store;
    let key_array = convert_byte_array(&mut env, &keys, key_num);
    let byte_pushdown = env.convert_byte_array(pushdown).unwrap();
    let ve_iter = store
        .get_multi_serialized_vertex_and_edge_with_filter_pushdown_buf(&key_array, &byte_pushdown);
    Box::into_raw(Box::new(ve_iter)) as jlong
}

/// helper function.

unsafe fn convert_byte_array(env: &mut JNIEnv, keys: &JObjectArray, key_num: jint) -> Vec<Vec<u8>> {
    let mut key_array = vec![];

    let mut i = 0;
    while i < key_num {
        let jobj = JByteArray::from(env.get_object_array_element(keys, i).unwrap());
        key_array.push(env.convert_byte_array(jobj).unwrap());
        i += 1;
    }
    key_array
}

fn jmap_to_hashmap(
    env: &mut JNIEnv,
    params: &JObject,
) -> Result<FxHashMap<String, String>, String> {
    let map = JMap::from_env(env, params).unwrap();
    let mut iter = map.iter(env).unwrap();

    let mut result: FxHashMap<String, String> = FxHashMap::default();
    while let Some(e) = iter.next(env).unwrap() {
        let k = JString::from(e.0);
        let v = JString::from(e.1);
        let key = env.get_string(&k).unwrap().into();
        let value = env.get_string(&v).unwrap().into();
        result.insert(key, value);
    }

    Ok(result)
}
