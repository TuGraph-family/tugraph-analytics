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

use jni::{
    objects::{JByteArray, JClass, JObject, JValue},
    sys::{jlong, jobject, jobjectArray},
    JNIEnv,
};

use crate::{
    api::{
        graph::serialized_vertex_and_edge::SerializedVertexAndEdge,
        iterator::{
            edge_iterator::EdgeIter, serialized_edge_iterator::SerializedEdgeIter,
            serialized_vertex_and_edge_iterator::SerializedVertexAndEdgeIter,
            serialized_vertex_iterator::SerializedVertexIter,
            vertex_and_edge_iterator::VertexAndEdgeIter, vertex_iterator::VertexIter,
        },
    },
    cstorejni::graph_cstore::VERTEX_AND_EDGE_CLAZZ,
};

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "system" fn Java_com_antgroup_geaflow_store_cstore_CStoreVertexIterator_next<
    'a,
>(
    env: JNIEnv<'a>,
    _: JClass<'a>,
    iter: *mut SerializedVertexIter<'a>,
) -> JByteArray<'a> {
    let v_iter = &mut *iter;
    let option_vertex = v_iter.next();
    if let Some(vertex) = option_vertex {
        env.byte_array_from_slice(vertex.as_slice()).unwrap()
    } else {
        JByteArray::default()
    }
}

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "system" fn Java_com_antgroup_geaflow_store_cstore_CStoreVertexIterator_disposeInternal(
    _: JNIEnv,
    _: JClass,
    iter: *mut VertexIter,
) {
    drop(Box::from_raw(iter));
}

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "system" fn Java_com_antgroup_geaflow_store_cstore_CStoreEdgeIterator_next<'a>(
    env: JNIEnv<'a>,
    _: JClass<'a>,
    iter: *mut SerializedEdgeIter<'a>,
) -> JByteArray<'a> {
    let e_iter = &mut *iter;
    let option_edge = e_iter.next();
    if let Some(edge) = option_edge {
        env.byte_array_from_slice(edge.as_slice()).unwrap()
    } else {
        JByteArray::default()
    }
}

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "system" fn Java_com_antgroup_geaflow_store_cstore_CStoreEdgeIterator_disposeInternal(
    _: JNIEnv,
    _: JClass,
    iter: *mut EdgeIter,
) {
    drop(Box::from_raw(iter));
}

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "system" fn Java_com_antgroup_geaflow_store_cstore_CStoreVertexAndEdgeIterator_next(
    env: JNIEnv,
    _: JClass,
    iter: *mut SerializedVertexAndEdgeIter,
) -> jobjectArray {
    let ve_iter = &mut *iter;
    let option_ve = ve_iter.next();
    if let Some(ve) = option_ve {
        vertex_and_edge_tran(env, ve)
    } else {
        JObject::null().as_raw()
    }
}

unsafe fn vertex_and_edge_tran(mut env: JNIEnv, ve: SerializedVertexAndEdge) -> jobject {
    let sid = JValue::Object(env.byte_array_from_slice(ve.src_id()).unwrap().as_ref()).as_jni();
    let v_iter = JValue::Long(Box::into_raw(Box::new(ve.vertex_iter)) as jlong).as_jni();
    let e_iter = JValue::Long(Box::into_raw(Box::new(ve.edge_iter)) as jlong).as_jni();
    let (class_ref, constructor_id) = VERTEX_AND_EDGE_CLAZZ.get().unwrap();
    let jclazz = JClass::from_raw(class_ref.as_raw());
    let obj = env.new_object_unchecked(jclazz, *constructor_id, &[sid, v_iter, e_iter]);
    obj.unwrap().as_raw()
}

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "system" fn Java_com_antgroup_geaflow_store_cstore_CStoreVertexAndEdgeIterator_disposeInternal(
    _: JNIEnv,
    _: JClass,
    iter: *mut VertexAndEdgeIter,
) {
    drop(Box::from_raw(iter));
}
