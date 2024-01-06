/*
 * Copyright 2023 AntGroup CO., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.antgroup.geaflow.store.cstore;

import com.antgroup.geaflow.store.cstore.Pushdown.EdgeLimit;
import com.antgroup.geaflow.store.cstore.Pushdown.FilterNode;
import com.antgroup.geaflow.store.cstore.Pushdown.FilterNodes;
import com.antgroup.geaflow.store.cstore.Pushdown.FilterType;
import com.antgroup.geaflow.store.cstore.Pushdown.LongList;
import com.antgroup.geaflow.store.cstore.Pushdown.PushDown;
import com.antgroup.geaflow.store.cstore.Pushdown.PushDown.Builder;
import com.antgroup.geaflow.store.cstore.Pushdown.StringList;
import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class NativeGraphStoreTest {

    @Test
    public void testLimit() {
        NativeGraphStore store = new NativeGraphStore("cstore", 0, new HashMap<>());
        for (int i = 0; i < 100; i++) {
            VertexContainer vertexContainer = new VertexContainer(
                ("a" + i).getBytes(), i, "foo", ("b" + i).getBytes());
            store.addVertex(vertexContainer);
            for (int j = 0; j < 100; j++) {
                EdgeContainer edgeContainer = new EdgeContainer(
                    ("a" + i).getBytes(), i, "foo", j % 2 == 0, ("d" + j).getBytes(), ("b" + i).getBytes());
                store.addEdge(edgeContainer);
            }
        }
        store.flush();

        int count = 0;
        try (CStoreEdgeIterator it = store.scanEdges(generateLimitPushDown(1, 1))) {
            while (it.hasNext()) {
                it.next();
                count++;
            }
        }
        Assert.assertEquals(count, 200);

        byte[][] keys = new byte[10][];
        for (int i = 0; i < 10; i++) {
            keys[i] = ("a" + i).getBytes();
        }

        count = 0;
        try (CStoreEdgeIterator it = store.scanEdges(keys, generateLimitPushDown(1, 1))) {
            while (it.hasNext()) {
                it.next();
                count++;
            }
        }
        Assert.assertEquals(count, 20);
    }


    private NativeGraphStore buildStore() {
        Map<String, String> config = new HashMap<>();
        config.put("hello", "world");
        config.put("foo", "bar");

        NativeGraphStore store = new NativeGraphStore("cstore", 0, config);

        for (int i = 0; i < 100; i++) {
            VertexContainer vertexContainer = new VertexContainer(
                ("a" + i).getBytes(), i, "foo", ("b" + i).getBytes());
            store.addVertex(vertexContainer);
            EdgeContainer edgeContainer = new EdgeContainer(
                ("a" + i).getBytes(), i, "foo", i % 2 == 0, "d".getBytes(), ("b" + i).getBytes());
            store.addEdge(edgeContainer);
        }
        store.flush();
        return store;
    }

    private void testPointGetWithoutPushDown(NativeGraphStore store) {
        for (int i = 0; i < 100; i++) {
            byte[] key = ("a" + i).getBytes();
            try (CStoreVertexIterator vertexIterator = store.getVertex(key)) {
                Assert.assertTrue(vertexIterator.hasNext());
                VertexContainer vertex = vertexIterator.next();
                Assert.assertEquals(vertex.label, "foo");
                Assert.assertEquals(vertex.ts, i);
                Assert.assertEquals(vertex.property, ("b" + i).getBytes());
            }
            try (CStoreEdgeIterator edgeIterator = store.getEdges(key)) {
                Assert.assertTrue(edgeIterator.hasNext());
                EdgeContainer edge = edgeIterator.next();
                Assert.assertEquals(edge.label, "foo");
                Assert.assertEquals(edge.ts, i);
                Assert.assertEquals(edge.isOut, i % 2 == 0);
                Assert.assertEquals(edge.tid, "d".getBytes());
                Assert.assertEquals(edge.property, ("b" + i).getBytes());
            }
            VertexAndEdge vertexAndEdge = store.getVertexAndEdge(key);
            Assert.assertEquals(vertexAndEdge.key, key);
            try (CStoreVertexIterator vertexIterator = vertexAndEdge.vertexIter) {
                Assert.assertTrue(vertexIterator.hasNext());
                VertexContainer vertex = vertexIterator.next();
                Assert.assertEquals(vertex.label, "foo");
                Assert.assertEquals(vertex.ts, i);
                Assert.assertEquals(vertex.property, ("b" + i).getBytes());
            }
            try (CStoreEdgeIterator edgeIterator = vertexAndEdge.edgeIter) {
                Assert.assertTrue(edgeIterator.hasNext());
                EdgeContainer edge = edgeIterator.next();
                Assert.assertEquals(edge.label, "foo");
                Assert.assertEquals(edge.ts, i);
                Assert.assertEquals(edge.isOut, i % 2 == 0);
                Assert.assertEquals(edge.tid, "d".getBytes());
                Assert.assertEquals(edge.property, ("b" + i).getBytes());
            }
        }
        byte[] key = ("a" + 100).getBytes();
        try (CStoreVertexIterator vertexIterator = store.getVertex(key)) {
            Assert.assertFalse(vertexIterator.hasNext());
        }
        try (CStoreEdgeIterator edgeIterator = store.getEdges(key)) {
            Assert.assertFalse(edgeIterator.hasNext());
        }
        VertexAndEdge vertexAndEdge = store.getVertexAndEdge(key);
        Assert.assertEquals(vertexAndEdge.key, key);
        try (CStoreVertexIterator vertexIterator = vertexAndEdge.vertexIter) {
            Assert.assertFalse(vertexIterator.hasNext());
        }
        try (CStoreEdgeIterator edgeIterator = vertexAndEdge.edgeIter) {
            Assert.assertFalse(edgeIterator.hasNext());
        }
    }

    private void testMultiPointGetWithoutPushDown(NativeGraphStore store) {
        byte[][] keys = new byte[100][];
        for (int i = 0; i < 100; i++) {
            keys[i] = ("a" + i).getBytes();
        }

        int count = 0;
        try (CStoreVertexIterator vertexIterator = store.scanVertex(keys)) {
            while (vertexIterator.hasNext()) {
                count++;
                VertexContainer container = vertexIterator.next();
                int idx = (int) container.ts;
                Assert.assertEquals(container.label, "foo");
                Assert.assertEquals(container.property, ("b" + idx).getBytes());
            }
        }
        Assert.assertEquals(count, 100);

        count = 0;
        try (CStoreEdgeIterator edgeIterator = store.scanEdges(keys)) {
            while (edgeIterator.hasNext()) {
                count++;
                EdgeContainer container = edgeIterator.next();
                int idx = (int) container.ts;
                Assert.assertEquals(container.label, "foo");
                Assert.assertEquals(container.isOut, idx % 2 == 0);
                Assert.assertEquals(container.tid, "d".getBytes());
                Assert.assertEquals(container.property, ("b" + idx).getBytes());
            }
        }
        Assert.assertEquals(count, 100);

        count = 0;
        try (CStoreVertexAndEdgeIterator iterator = store.scanVertexAndEdge(keys)) {
            while (iterator.hasNext()) {
                count++;
                VertexAndEdgeContainer vertexAndEdge = iterator.next();
                int idx = Integer.parseInt(new String(vertexAndEdge.sid).substring(1));
                try (CStoreVertexIterator vertexIterator = new CStoreVertexIterator(vertexAndEdge.vertexIt)) {
                    Assert.assertTrue(vertexIterator.hasNext());
                    VertexContainer vertex = vertexIterator.next();
                    Assert.assertEquals(vertex.label, "foo");
                    Assert.assertEquals(vertex.ts, idx);
                    Assert.assertEquals(vertex.property, ("b" + idx).getBytes());
                }
                try (CStoreEdgeIterator edgeIterator = new CStoreEdgeIterator(vertexAndEdge.edgeIt)) {
                    Assert.assertTrue(edgeIterator.hasNext());
                    EdgeContainer edge = edgeIterator.next();
                    Assert.assertEquals(edge.label, "foo");
                    Assert.assertEquals(edge.ts, idx);
                    Assert.assertEquals(edge.isOut, idx % 2 == 0);
                    Assert.assertEquals(edge.tid, "d".getBytes());
                    Assert.assertEquals(edge.property, ("b" + idx).getBytes());
                }
            }
        }
        Assert.assertEquals(count, 100);
    }

    private void testScanWithoutPushDown(NativeGraphStore store) {
        int count = 0;
        try (CStoreVertexIterator vertexIterator = store.scanVertex()) {
            while (vertexIterator.hasNext()) {
                count++;
                VertexContainer container = vertexIterator.next();
                int idx = Integer.parseInt(new String(container.id).substring(1));
                Assert.assertEquals(container.label, "foo");
                Assert.assertEquals(container.ts, idx);
                Assert.assertEquals(container.property, ("b" + idx).getBytes());
            }
        }
        Assert.assertEquals(count, 100);

        count = 0;
        try (CStoreEdgeIterator edgeIterator = store.scanEdges()) {
            while (edgeIterator.hasNext()) {
                count++;
                EdgeContainer container = edgeIterator.next();
                int idx = Integer.parseInt(new String(container.sid).substring(1));
                Assert.assertEquals(container.label, "foo");
                Assert.assertEquals(container.ts, idx);
                Assert.assertEquals(container.isOut, idx % 2 == 0);
                Assert.assertEquals(container.tid, "d".getBytes());
                Assert.assertEquals(container.property, ("b" + idx).getBytes());
            }
        }
        Assert.assertEquals(count, 100);

        count = 0;
        try (CStoreVertexAndEdgeIterator iterator = store.scanVertexAndEdge()) {
            while (iterator.hasNext()) {
                count++;
                VertexAndEdgeContainer vertexAndEdge = iterator.next();
                int idx = Integer.parseInt(new String(vertexAndEdge.sid).substring(1));
                try (CStoreVertexIterator vertexIterator = new CStoreVertexIterator(vertexAndEdge.vertexIt)) {
                    Assert.assertTrue(vertexIterator.hasNext());
                    VertexContainer vertex = vertexIterator.next();
                    Assert.assertEquals(vertex.label, "foo");
                    Assert.assertEquals(vertex.ts, idx);
                    Assert.assertEquals(vertex.property, ("b" + idx).getBytes());
                }
                try (CStoreEdgeIterator edgeIterator = new CStoreEdgeIterator(vertexAndEdge.edgeIt)) {
                    Assert.assertTrue(edgeIterator.hasNext());
                    EdgeContainer edge = edgeIterator.next();
                    Assert.assertEquals(edge.label, "foo");
                    Assert.assertEquals(edge.ts, idx);
                    Assert.assertEquals(edge.isOut, idx % 2 == 0);
                    Assert.assertEquals(edge.tid, "d".getBytes());
                    Assert.assertEquals(edge.property, ("b" + idx).getBytes());
                }
            }
        }
        Assert.assertEquals(count, 100);
    }

    private void testPointGetVertexWithPushDown(NativeGraphStore store, FilterType filterType) {
        byte[] pushDown = generatePushDown(filterType, null);
        switch (filterType) {
            case EMPTY:
            case ONLY_VERTEX:
            case OR:
                for (int i = 0; i < 200; i++) {
                    byte[] key = ("a" + i).getBytes();
                    try (CStoreVertexIterator vertexIterator = store.getVertex(key, pushDown)) {
                        Assert.assertTrue(vertexIterator.hasNext());
                        VertexContainer vertex = vertexIterator.next();
                        Assert.assertTrue(
                            i < 100 && vertex.label.equals("foo") || vertex.label.equals(
                                "foo" + i));
                        Assert.assertEquals(vertex.ts, i);
                        Assert.assertEquals(vertex.property, ("b" + i).getBytes());
                    }
                }
                break;
            case VERTEX_VALUE_DROP:
                for (int i = 0; i < 200; i++) {
                    byte[] key = ("a" + i).getBytes();
                    try (CStoreVertexIterator vertexIterator = store.getVertex(key, pushDown)) {
                        Assert.assertTrue(vertexIterator.hasNext());
                        VertexContainer vertex = vertexIterator.next();
                        Assert.assertTrue(
                            i < 100 && vertex.label.equals("foo") || vertex.label.equals(
                                "foo" + i));
                        Assert.assertEquals(vertex.ts, i);
                        Assert.assertEquals(vertex.property, new byte[0]);
                    }
                }
                break;
            case AND:
            case VERTEX_TS:
                for (int i = 0; i < 200; i++) {
                    byte[] key = ("a" + i).getBytes();
                    try (CStoreVertexIterator vertexIterator = store.getVertex(key, pushDown)) {
                        if (i < 10) {
                            Assert.assertTrue(vertexIterator.hasNext());
                            VertexContainer vertex = vertexIterator.next();
                            Assert.assertTrue(
                                vertex.label.equals("foo") || vertex.label.equals("foo" + i));
                            Assert.assertEquals(vertex.ts, i);
                            Assert.assertEquals(vertex.property, ("b" + i).getBytes());
                        } else {
                            Assert.assertFalse(vertexIterator.hasNext());
                        }
                    }
                }
                break;
            case VERTEX_LABEL:
                for (int i = 0; i < 200; i++) {
                    byte[] key = ("a" + i).getBytes();
                    try (CStoreVertexIterator vertexIterator = store.getVertex(key, pushDown)) {
                        if (i < 100) {
                            Assert.assertTrue(vertexIterator.hasNext());
                            VertexContainer vertex = vertexIterator.next();
                            Assert.assertEquals(vertex.label, "foo");
                            Assert.assertEquals(vertex.ts, i);
                            Assert.assertEquals(vertex.property, ("b" + i).getBytes());
                        } else {
                            Assert.assertFalse(vertexIterator.hasNext());
                        }
                    }
                }
                break;
            default:
                throw new RuntimeException("unsupported filter type " + filterType.name());
        }
    }

    private void testPointGetEdgesWithPushDown(NativeGraphStore store, FilterType filterType) {
        byte[] pushDown = generatePushDown(filterType, null);
        switch (filterType) {
            case EMPTY:
            case OR:
                for (int i = 0; i < 200; i++) {
                    byte[] key = ("a" + i).getBytes();
                    try (CStoreEdgeIterator edgeIterator = store.getEdges(key, pushDown)) {
                        Assert.assertTrue(edgeIterator.hasNext());
                        EdgeContainer edge = edgeIterator.next();
                        Assert.assertTrue(i < 100 && edge.label.equals("foo") || edge.label.equals("foo" + i));
                        Assert.assertEquals(edge.ts, i);
                        Assert.assertEquals(edge.isOut, i % 2 == 0);
                        Assert.assertEquals(edge.tid, "d".getBytes());
                        Assert.assertEquals(edge.property, ("b" + i).getBytes());
                    }
                }
                break;
            case IN_EDGE:
                for (int i = 0; i < 200; i++) {
                    byte[] key = ("a" + i).getBytes();
                    try (CStoreEdgeIterator edgeIterator = store.getEdges(key, pushDown)) {
                        if (i % 2 == 1) {
                            Assert.assertTrue(edgeIterator.hasNext());
                            EdgeContainer edge = edgeIterator.next();
                            Assert.assertTrue(i < 100 && edge.label.equals("foo") || edge.label.equals("foo" + i));
                            Assert.assertEquals(edge.ts, i);
                            Assert.assertFalse(edge.isOut);
                            Assert.assertEquals(edge.tid, "d".getBytes());
                            Assert.assertEquals(edge.property, ("b" + i).getBytes());
                        } else {
                            Assert.assertFalse(edgeIterator.hasNext());
                        }
                    }
                }
                break;
            case OUT_EDGE:
            case AND:
                for (int i = 0; i < 200; i++) {
                    byte[] key = ("a" + i).getBytes();
                    try (CStoreEdgeIterator edgeIterator = store.getEdges(key, pushDown)) {
                        if (i % 2 == 0) {
                            Assert.assertTrue(edgeIterator.hasNext());
                            EdgeContainer edge = edgeIterator.next();
                            Assert.assertTrue(i < 100 && edge.label.equals("foo") || edge.label.equals("foo" + i));
                            Assert.assertEquals(edge.ts, i);
                            Assert.assertTrue(edge.isOut);
                            Assert.assertEquals(edge.tid, "d".getBytes());
                            Assert.assertEquals(edge.property, ("b" + i).getBytes());
                        } else {
                            Assert.assertFalse(edgeIterator.hasNext());
                        }
                    }
                }
                break;
            case MULTI_EDGE_TS:
                for (int i = 0; i < 200; i++) {
                    byte[] key = ("a" + i).getBytes();
                    try (CStoreEdgeIterator edgeIterator = store.getEdges(key, pushDown)) {
                        if (i < 20) {
                            Assert.assertTrue(edgeIterator.hasNext());
                            EdgeContainer edge = edgeIterator.next();
                            Assert.assertTrue(i < 100 && edge.label.equals("foo") || edge.label.equals("foo" + i));
                            Assert.assertEquals(edge.ts, i);
                            Assert.assertEquals(edge.isOut, i % 2 == 0);
                            Assert.assertEquals(edge.tid, "d".getBytes());
                            Assert.assertEquals(edge.property, ("b" + i).getBytes());
                        } else {
                            Assert.assertFalse(edgeIterator.hasNext());
                        }
                    }
                }
                break;
            case EDGE_VALUE_DROP:
                for (int i = 0; i < 200; i++) {
                    byte[] key = ("a" + i).getBytes();
                    try (CStoreEdgeIterator edgeIterator = store.getEdges(key, pushDown)) {
                        Assert.assertTrue(edgeIterator.hasNext());
                        EdgeContainer edge = edgeIterator.next();
                        Assert.assertTrue(i < 100 && edge.label.equals("foo") || edge.label.equals("foo" + i));
                        Assert.assertEquals(edge.ts, i);
                        Assert.assertEquals(edge.isOut, i % 2 == 0);
                        Assert.assertEquals(edge.tid, "d".getBytes());
                        Assert.assertEquals(edge.property, new byte[0]);
                    }
                }
                break;
            case EDGE_TS:
                for (int i = 0; i < 200; i++) {
                    byte[] key = ("a" + i).getBytes();
                    try (CStoreEdgeIterator edgeIterator = store.getEdges(key, pushDown)) {
                        if (i < 10) {
                            Assert.assertTrue(edgeIterator.hasNext());
                            EdgeContainer edge = edgeIterator.next();
                            Assert.assertTrue(edge.label.equals("foo") || edge.label.equals("foo" + i));
                            Assert.assertEquals(edge.ts, i);
                            Assert.assertEquals(edge.isOut, i % 2 == 0);
                            Assert.assertEquals(edge.tid, "d".getBytes());
                            Assert.assertEquals(edge.property, ("b" + i).getBytes());
                        } else {
                            Assert.assertFalse(edgeIterator.hasNext());
                        }
                    }
                }
                break;
            case EDGE_LABEL:
                for (int i = 0; i < 200; i++) {
                    byte[] key = ("a" + i).getBytes();
                    try (CStoreEdgeIterator edgeIterator = store.getEdges(key, pushDown)) {
                        if (i < 100) {
                            Assert.assertTrue(edgeIterator.hasNext());
                            EdgeContainer edge = edgeIterator.next();
                            Assert.assertTrue(edge.label.equals("foo") || edge.label.equals("foo" + i));
                            Assert.assertEquals(edge.ts, i);
                            Assert.assertEquals(edge.isOut, i % 2 == 0);
                            Assert.assertEquals(edge.tid, "d".getBytes());
                            Assert.assertEquals(edge.property, ("b" + i).getBytes());
                        } else {
                            Assert.assertFalse(edgeIterator.hasNext());
                        }
                    }
                }
                break;
            default:
                throw new RuntimeException("unsupported filter type " + filterType.name());
        }
    }

    private void testPointGetVertexAndEdgeWithPushDown(NativeGraphStore store,
                                                       FilterType filterType) {
        byte[] pushDown = generatePushDown(filterType, null);
        switch (filterType) {
            case EMPTY:
            case VERTEX_MUST_CONTAIN:
            case OR:
                for (int i = 0; i < 200; i++) {
                    byte[] key = ("a" + i).getBytes();
                    VertexAndEdge vertexAndEdge = store.getVertexAndEdge(key, pushDown);
                    Assert.assertEquals(vertexAndEdge.key, key);
                    try (CStoreVertexIterator vertexIterator = vertexAndEdge.vertexIter) {
                        Assert.assertTrue(vertexIterator.hasNext());
                        VertexContainer vertex = vertexIterator.next();
                        Assert.assertTrue(i < 100 && vertex.label.equals("foo") || vertex.label.equals("foo" + i));
                        Assert.assertEquals(vertex.ts, i);
                        Assert.assertEquals(vertex.property, ("b" + i).getBytes());
                    }
                    try (CStoreEdgeIterator edgeIterator = vertexAndEdge.edgeIter) {
                        Assert.assertTrue(edgeIterator.hasNext());
                        EdgeContainer edge = edgeIterator.next();
                        Assert.assertTrue(i < 100 && edge.label.equals("foo") || edge.label.equals("foo" + i));
                        Assert.assertEquals(edge.ts, i);
                        Assert.assertEquals(edge.isOut, i % 2 == 0);
                        Assert.assertEquals(edge.tid, "d".getBytes());
                        Assert.assertEquals(edge.property, ("b" + i).getBytes());
                    }
                }
                break;
            case ONLY_VERTEX:
                for (int i = 0; i < 200; i++) {
                    byte[] key = ("a" + i).getBytes();
                    VertexAndEdge vertexAndEdge = store.getVertexAndEdge(key, pushDown);
                    Assert.assertEquals(vertexAndEdge.key, key);
                    try (CStoreVertexIterator vertexIterator = vertexAndEdge.vertexIter) {
                        Assert.assertTrue(vertexIterator.hasNext());
                        VertexContainer vertex = vertexIterator.next();
                        Assert.assertTrue(i < 100 && vertex.label.equals("foo") || vertex.label.equals("foo" + i));
                        Assert.assertEquals(vertex.ts, i);
                        Assert.assertEquals(vertex.property, ("b" + i).getBytes());
                    }
                    try (CStoreEdgeIterator edgeIterator = vertexAndEdge.edgeIter) {
                        Assert.assertFalse(edgeIterator.hasNext());
                    }
                }
                break;
            case IN_EDGE:
                for (int i = 0; i < 200; i++) {
                    byte[] key = ("a" + i).getBytes();
                    VertexAndEdge vertexAndEdge = store.getVertexAndEdge(key, pushDown);
                    Assert.assertEquals(vertexAndEdge.key, key);
                    try (CStoreVertexIterator vertexIterator = vertexAndEdge.vertexIter) {
                        Assert.assertTrue(vertexIterator.hasNext());
                        VertexContainer vertex = vertexIterator.next();
                        Assert.assertTrue(i < 100 && vertex.label.equals("foo") || vertex.label.equals("foo" + i));
                        Assert.assertEquals(vertex.ts, i);
                        Assert.assertEquals(vertex.property, ("b" + i).getBytes());
                    }
                    try (CStoreEdgeIterator edgeIterator = vertexAndEdge.edgeIter) {
                        if (i % 2 == 1) {
                            Assert.assertTrue(edgeIterator.hasNext());
                            EdgeContainer edge = edgeIterator.next();
                            Assert.assertTrue(i < 100 && edge.label.equals("foo") || edge.label.equals("foo" + i));
                            Assert.assertEquals(edge.ts, i);
                            Assert.assertFalse(edge.isOut);
                            Assert.assertEquals(edge.tid, "d".getBytes());
                            Assert.assertEquals(edge.property, ("b" + i).getBytes());
                        } else {
                            Assert.assertFalse(edgeIterator.hasNext());
                        }
                    }
                }
                break;
            case OUT_EDGE:
                for (int i = 0; i < 200; i++) {
                    byte[] key = ("a" + i).getBytes();
                    VertexAndEdge vertexAndEdge = store.getVertexAndEdge(key, pushDown);
                    Assert.assertEquals(vertexAndEdge.key, key);
                    try (CStoreVertexIterator vertexIterator = vertexAndEdge.vertexIter) {
                        Assert.assertTrue(vertexIterator.hasNext());
                        VertexContainer vertex = vertexIterator.next();
                        Assert.assertTrue(i < 100 && vertex.label.equals("foo") || vertex.label.equals("foo" + i));
                        Assert.assertEquals(vertex.ts, i);
                        Assert.assertEquals(vertex.property, ("b" + i).getBytes());
                    }
                    try (CStoreEdgeIterator edgeIterator = vertexAndEdge.edgeIter) {
                        if (i % 2 == 0) {
                            Assert.assertTrue(edgeIterator.hasNext());
                            EdgeContainer edge = edgeIterator.next();
                            Assert.assertTrue(i < 100 && edge.label.equals("foo") || edge.label.equals("foo" + i));
                            Assert.assertEquals(edge.ts, i);
                            Assert.assertTrue(edge.isOut);
                            Assert.assertEquals(edge.tid, "d".getBytes());
                            Assert.assertEquals(edge.property, ("b" + i).getBytes());
                        } else {
                            Assert.assertFalse(edgeIterator.hasNext());
                        }
                    }
                }
                break;
            case VERTEX_VALUE_DROP:
                for (int i = 0; i < 200; i++) {
                    byte[] key = ("a" + i).getBytes();
                    VertexAndEdge vertexAndEdge = store.getVertexAndEdge(key, pushDown);
                    Assert.assertEquals(vertexAndEdge.key, key);
                    try (CStoreVertexIterator vertexIterator = vertexAndEdge.vertexIter) {
                        Assert.assertTrue(vertexIterator.hasNext());
                        VertexContainer vertex = vertexIterator.next();
                        Assert.assertTrue(i < 100 && vertex.label.equals("foo") || vertex.label.equals("foo" + i));
                        Assert.assertEquals(vertex.ts, i);
                        Assert.assertEquals(vertex.property, new byte[0]);
                    }
                    try (CStoreEdgeIterator edgeIterator = vertexAndEdge.edgeIter) {
                        Assert.assertTrue(edgeIterator.hasNext());
                        EdgeContainer edge = edgeIterator.next();
                        Assert.assertTrue(i < 100 && edge.label.equals("foo") || edge.label.equals("foo" + i));
                        Assert.assertEquals(edge.ts, i);
                        Assert.assertEquals(edge.isOut, i % 2 == 0);
                        Assert.assertEquals(edge.tid, "d".getBytes());
                        Assert.assertEquals(edge.property, ("b" + i).getBytes());
                    }
                }
                break;
            case EDGE_VALUE_DROP:
                for (int i = 0; i < 200; i++) {
                    byte[] key = ("a" + i).getBytes();
                    VertexAndEdge vertexAndEdge = store.getVertexAndEdge(key, pushDown);
                    Assert.assertEquals(vertexAndEdge.key, key);
                    try (CStoreVertexIterator vertexIterator = vertexAndEdge.vertexIter) {
                        Assert.assertTrue(vertexIterator.hasNext());
                        VertexContainer vertex = vertexIterator.next();
                        Assert.assertTrue(i < 100 && vertex.label.equals("foo") || vertex.label.equals("foo" + i));
                        Assert.assertEquals(vertex.ts, i);
                        Assert.assertEquals(vertex.property, ("b" + i).getBytes());
                    }
                    try (CStoreEdgeIterator edgeIterator = vertexAndEdge.edgeIter) {
                        Assert.assertTrue(edgeIterator.hasNext());
                        EdgeContainer edge = edgeIterator.next();
                        Assert.assertTrue(i < 100 && edge.label.equals("foo") || edge.label.equals("foo" + i));
                        Assert.assertEquals(edge.ts, i);
                        Assert.assertEquals(edge.isOut, i % 2 == 0);
                        Assert.assertEquals(edge.tid, "d".getBytes());
                        Assert.assertEquals(edge.property, new byte[0]);
                    }
                }
                break;
            case AND:
                for (int i = 0; i < 200; i++) {
                    byte[] key = ("a" + i).getBytes();
                    VertexAndEdge vertexAndEdge = store.getVertexAndEdge(key, pushDown);
                    Assert.assertEquals(vertexAndEdge.key, key);
                    try (CStoreVertexIterator vertexIterator = vertexAndEdge.vertexIter) {
                        if (i < 10) {
                            Assert.assertTrue(vertexIterator.hasNext());
                            VertexContainer vertex = vertexIterator.next();
                            Assert.assertTrue(vertex.label.equals("foo"));
                            Assert.assertEquals(vertex.ts, i);
                            Assert.assertEquals(vertex.property, ("b" + i).getBytes());
                        } else {
                            Assert.assertFalse(vertexIterator.hasNext());
                        }
                    }
                    try (CStoreEdgeIterator edgeIterator = vertexAndEdge.edgeIter) {
                        if (i % 2 == 0) {
                            Assert.assertTrue(edgeIterator.hasNext());
                            EdgeContainer edge = edgeIterator.next();
                            Assert.assertTrue(i < 100 && edge.label.equals("foo") || edge.label.equals("foo" + i));
                            Assert.assertEquals(edge.ts, i);
                            Assert.assertTrue(edge.isOut);
                            Assert.assertEquals(edge.tid, "d".getBytes());
                            Assert.assertEquals(edge.property, ("b" + i).getBytes());
                        } else {
                            Assert.assertFalse(edgeIterator.hasNext());
                        }
                    }
                }
                break;
            case VERTEX_TS:
                for (int i = 0; i < 200; i++) {
                    byte[] key = ("a" + i).getBytes();
                    VertexAndEdge vertexAndEdge = store.getVertexAndEdge(key, pushDown);
                    Assert.assertEquals(vertexAndEdge.key, key);
                    try (CStoreVertexIterator vertexIterator = vertexAndEdge.vertexIter) {
                        if (i < 10) {
                            Assert.assertTrue(vertexIterator.hasNext());
                            VertexContainer vertex = vertexIterator.next();
                            Assert.assertTrue(vertex.label.equals("foo") || vertex.label.equals("foo" + i));
                            Assert.assertEquals(vertex.ts, i);
                            Assert.assertEquals(vertex.property, ("b" + i).getBytes());
                        } else {
                            Assert.assertFalse(vertexIterator.hasNext());
                        }
                    }
                    try (CStoreEdgeIterator edgeIterator = vertexAndEdge.edgeIter) {
                        Assert.assertTrue(edgeIterator.hasNext());
                        EdgeContainer edge = edgeIterator.next();
                        Assert.assertTrue(i < 100 && edge.label.equals("foo") || edge.label.equals("foo" + i));
                        Assert.assertEquals(edge.ts, i);
                        Assert.assertEquals(edge.isOut, i % 2 == 0);
                        Assert.assertEquals(edge.tid, "d".getBytes());
                        Assert.assertEquals(edge.property, ("b" + i).getBytes());
                    }
                }
                break;
            case EDGE_TS:
                for (int i = 0; i < 200; i++) {
                    byte[] key = ("a" + i).getBytes();
                    VertexAndEdge vertexAndEdge = store.getVertexAndEdge(key, pushDown);
                    Assert.assertEquals(vertexAndEdge.key, key);
                    try (CStoreVertexIterator vertexIterator = vertexAndEdge.vertexIter) {
                        Assert.assertTrue(vertexIterator.hasNext());
                        VertexContainer vertex = vertexIterator.next();
                        Assert.assertTrue(i < 100 && vertex.label.equals("foo") || vertex.label.equals("foo" + i));
                        Assert.assertEquals(vertex.ts, i);
                        Assert.assertEquals(vertex.property, ("b" + i).getBytes());
                    }
                    try (CStoreEdgeIterator edgeIterator = vertexAndEdge.edgeIter) {
                        if (i < 10) {
                            Assert.assertTrue(edgeIterator.hasNext());
                            EdgeContainer edge = edgeIterator.next();
                            Assert.assertTrue(edge.label.equals("foo") || edge.label.equals("foo" + i));
                            Assert.assertEquals(edge.ts, i);
                            Assert.assertEquals(edge.isOut, i % 2 == 0);
                            Assert.assertEquals(edge.tid, "d".getBytes());
                            Assert.assertEquals(edge.property, ("b" + i).getBytes());
                        } else {
                            Assert.assertFalse(edgeIterator.hasNext());
                        }
                    }
                }
                break;
            case MULTI_EDGE_TS:
                for (int i = 0; i < 200; i++) {
                    byte[] key = ("a" + i).getBytes();
                    VertexAndEdge vertexAndEdge = store.getVertexAndEdge(key, pushDown);
                    Assert.assertEquals(vertexAndEdge.key, key);
                    try (CStoreVertexIterator vertexIterator = vertexAndEdge.vertexIter) {
                        Assert.assertTrue(vertexIterator.hasNext());
                        VertexContainer vertex = vertexIterator.next();
                        Assert.assertTrue(i < 100 && vertex.label.equals("foo") || vertex.label.equals("foo" + i));
                        Assert.assertEquals(vertex.ts, i);
                        Assert.assertEquals(vertex.property, ("b" + i).getBytes());
                    }
                    try (CStoreEdgeIterator edgeIterator = vertexAndEdge.edgeIter) {
                        if (i < 20) {
                            Assert.assertTrue(edgeIterator.hasNext());
                            EdgeContainer edge = edgeIterator.next();
                            Assert.assertTrue(edge.label.equals("foo") || edge.label.equals("foo" + i));
                            Assert.assertEquals(edge.ts, i);
                            Assert.assertEquals(edge.isOut, i % 2 == 0);
                            Assert.assertEquals(edge.tid, "d".getBytes());
                            Assert.assertEquals(edge.property, ("b" + i).getBytes());
                        } else {
                            Assert.assertFalse(edgeIterator.hasNext());
                        }
                    }
                }
                break;
            case VERTEX_LABEL:
                for (int i = 0; i < 200; i++) {
                    byte[] key = ("a" + i).getBytes();
                    VertexAndEdge vertexAndEdge = store.getVertexAndEdge(key, pushDown);
                    Assert.assertEquals(vertexAndEdge.key, key);
                    try (CStoreVertexIterator vertexIterator = vertexAndEdge.vertexIter) {
                        if (i < 100) {
                            Assert.assertTrue(vertexIterator.hasNext());
                            VertexContainer vertex = vertexIterator.next();
                            Assert.assertTrue(vertex.label.equals("foo") || vertex.label.equals("foo" + i));
                            Assert.assertEquals(vertex.ts, i);
                            Assert.assertEquals(vertex.property, ("b" + i).getBytes());
                        } else {
                            Assert.assertFalse(vertexIterator.hasNext());
                        }
                    }
                    try (CStoreEdgeIterator edgeIterator = vertexAndEdge.edgeIter) {
                        Assert.assertTrue(edgeIterator.hasNext());
                        EdgeContainer edge = edgeIterator.next();
                        Assert.assertTrue(i < 100 && edge.label.equals("foo") || edge.label.equals("foo" + i));
                        Assert.assertEquals(edge.ts, i);
                        Assert.assertEquals(edge.isOut, i % 2 == 0);
                        Assert.assertEquals(edge.tid, "d".getBytes());
                        Assert.assertEquals(edge.property, ("b" + i).getBytes());
                    }
                }
                break;
            case EDGE_LABEL:
                for (int i = 0; i < 200; i++) {
                    byte[] key = ("a" + i).getBytes();
                    VertexAndEdge vertexAndEdge = store.getVertexAndEdge(key, pushDown);
                    Assert.assertEquals(vertexAndEdge.key, key);
                    try (CStoreVertexIterator vertexIterator = vertexAndEdge.vertexIter) {
                        Assert.assertTrue(vertexIterator.hasNext());
                        VertexContainer vertex = vertexIterator.next();
                        Assert.assertTrue(i < 100 && vertex.label.equals("foo") || vertex.label.equals("foo" + i));
                        Assert.assertEquals(vertex.ts, i);
                        Assert.assertEquals(vertex.property, ("b" + i).getBytes());
                    }
                    try (CStoreEdgeIterator edgeIterator = vertexAndEdge.edgeIter) {
                        if (i < 100) {
                            Assert.assertTrue(edgeIterator.hasNext());
                            EdgeContainer edge = edgeIterator.next();
                            Assert.assertTrue(edge.label.equals("foo") || edge.label.equals("foo" + i));
                            Assert.assertEquals(edge.ts, i);
                            Assert.assertEquals(edge.isOut, i % 2 == 0);
                            Assert.assertEquals(edge.tid, "d".getBytes());
                            Assert.assertEquals(edge.property, ("b" + i).getBytes());
                        } else {
                            Assert.assertFalse(edgeIterator.hasNext());
                        }
                    }
                }
                break;
            default:
                throw new RuntimeException("unsupported filter type " + filterType.name());
        }
    }

    private void testMultiPointGetVertexWithPushDown(NativeGraphStore store,
                                                     FilterType filterType) {
        byte[][] keys = new byte[200][];
        for (int i = 0; i < 200; i++) {
            keys[i] = ("a" + i).getBytes();
        }
        byte[] pushDown = generatePushDown(filterType, keys);
        int count = 0;
        switch (filterType) {
            case EMPTY:
            case ONLY_VERTEX:
            case OR:
                try (CStoreVertexIterator vertexIterator = store.scanVertex(keys, pushDown)) {
                    while (vertexIterator.hasNext()) {
                        count++;
                        VertexContainer container = vertexIterator.next();
                        int idx = (int) container.ts;
                        Assert.assertTrue(
                            idx < 100 && container.label.equals("foo") || container.label.equals(
                                "foo" + idx));
                        Assert.assertEquals(container.property, ("b" + idx).getBytes());
                    }
                }
                Assert.assertEquals(count, 200);
                break;
            case VERTEX_VALUE_DROP:
                try (CStoreVertexIterator vertexIterator = store.scanVertex(keys, pushDown)) {
                    while (vertexIterator.hasNext()) {
                        count++;
                        VertexContainer container = vertexIterator.next();
                        int idx = (int) container.ts;
                        Assert.assertTrue(
                            idx < 100 && container.label.equals("foo") || container.label.equals(
                                "foo" + idx));
                        Assert.assertEquals(container.property, new byte[0]);
                    }
                }
                Assert.assertEquals(count, 200);
                break;
            case AND:
            case VERTEX_TS:
                try (CStoreVertexIterator vertexIterator = store.scanVertex(keys, pushDown)) {
                    while (vertexIterator.hasNext()) {
                        count++;
                        VertexContainer container = vertexIterator.next();
                        int idx = (int) container.ts;
                        Assert.assertTrue(idx >= 0 && idx < 10);
                        Assert.assertEquals(container.label, "foo");
                        Assert.assertEquals(container.property, ("b" + idx).getBytes());
                    }
                }
                Assert.assertEquals(count, 10);
                break;
            case VERTEX_LABEL:
                try (CStoreVertexIterator vertexIterator = store.scanVertex(keys, pushDown)) {
                    while (vertexIterator.hasNext()) {
                        count++;
                        VertexContainer container = vertexIterator.next();
                        int idx = (int) container.ts;
                        Assert.assertTrue(idx >= 0 && idx < 100);
                        Assert.assertEquals(container.label, "foo");
                        Assert.assertEquals(container.property, ("b" + idx).getBytes());
                    }
                }
                Assert.assertEquals(count, 100);
                break;
            default:
                throw new RuntimeException("unsupported filter type " + filterType.name());
        }
    }

    private void testMultiPointGetEdgesWithPushDown(NativeGraphStore store, FilterType filterType) {
        byte[][] keys = new byte[200][];
        for (int i = 0; i < 200; i++) {
            keys[i] = ("a" + i).getBytes();
        }
        byte[] pushDown = generatePushDown(filterType, keys);
        int count = 0;
        switch (filterType) {
            case EMPTY:
            case OR:
                try (CStoreEdgeIterator edgeIterator = store.scanEdges(keys, pushDown)) {
                    while (edgeIterator.hasNext()) {
                        count++;
                        EdgeContainer container = edgeIterator.next();
                        int idx = (int) container.ts;
                        Assert.assertTrue(idx < 100 && container.label.equals("foo") || container.label.equals("foo" + idx));
                        Assert.assertEquals(container.isOut, idx % 2 == 0);
                        Assert.assertEquals(container.tid, "d".getBytes());
                        Assert.assertEquals(container.property, ("b" + idx).getBytes());
                    }
                }
                Assert.assertEquals(count, 200);
                break;
            case IN_EDGE:
                try (CStoreEdgeIterator edgeIterator = store.scanEdges(keys, pushDown)) {
                    while (edgeIterator.hasNext()) {
                        count++;
                        EdgeContainer container = edgeIterator.next();
                        int idx = (int) container.ts;
                        Assert.assertTrue(idx < 100 && container.label.equals("foo") || container.label.equals("foo" + idx));
                        Assert.assertFalse(container.isOut);
                        Assert.assertEquals(container.tid, "d".getBytes());
                        Assert.assertEquals(container.property, ("b" + idx).getBytes());
                    }
                }
                Assert.assertEquals(count, 100);
                break;
            case OUT_EDGE:
            case AND:
                try (CStoreEdgeIterator edgeIterator = store.scanEdges(keys, pushDown)) {
                    while (edgeIterator.hasNext()) {
                        count++;
                        EdgeContainer container = edgeIterator.next();
                        int idx = (int) container.ts;
                        Assert.assertTrue(idx < 100 && container.label.equals("foo") || container.label.equals("foo" + idx));
                        Assert.assertTrue(container.isOut);
                        Assert.assertEquals(container.tid, "d".getBytes());
                        Assert.assertEquals(container.property, ("b" + idx).getBytes());
                    }
                }
                Assert.assertEquals(count, 100);
                break;
            case MULTI_EDGE_TS:
                try (CStoreEdgeIterator edgeIterator = store.scanEdges(keys, pushDown)) {
                    while (edgeIterator.hasNext()) {
                        count++;
                        EdgeContainer container = edgeIterator.next();
                        int idx = (int) container.ts;
                        Assert.assertTrue(idx < 20 && container.label.equals("foo"));
                        Assert.assertEquals(container.isOut, idx % 2 == 0);
                        Assert.assertEquals(container.tid, "d".getBytes());
                        Assert.assertEquals(container.property, ("b" + idx).getBytes());
                    }
                }
                Assert.assertEquals(count, 20);
                break;
            case EDGE_VALUE_DROP:
                try (CStoreEdgeIterator edgeIterator = store.scanEdges(keys, pushDown)) {
                    while (edgeIterator.hasNext()) {
                        count++;
                        EdgeContainer container = edgeIterator.next();
                        int idx = (int) container.ts;
                        Assert.assertTrue(idx < 100 && container.label.equals("foo") || container.label.equals("foo" + idx));
                        Assert.assertEquals(container.isOut, idx % 2 == 0);
                        Assert.assertEquals(container.tid, "d".getBytes());
                        Assert.assertEquals(container.property, new byte[0]);
                    }
                }
                Assert.assertEquals(count, 200);
                break;
            case EDGE_TS:
                try (CStoreEdgeIterator edgeIterator = store.scanEdges(keys, pushDown)) {
                    while (edgeIterator.hasNext()) {
                        count++;
                        EdgeContainer container = edgeIterator.next();
                        int idx = (int) container.ts;
                        Assert.assertTrue(idx < 10 && container.label.equals("foo"));
                        Assert.assertEquals(container.isOut, idx % 2 == 0);
                        Assert.assertEquals(container.tid, "d".getBytes());
                        Assert.assertEquals(container.property, ("b" + idx).getBytes());
                    }
                }
                Assert.assertEquals(count, 10);
                break;
            case EDGE_LABEL:
                try (CStoreEdgeIterator edgeIterator = store.scanEdges(keys, pushDown)) {
                    while (edgeIterator.hasNext()) {
                        count++;
                        EdgeContainer container = edgeIterator.next();
                        int idx = (int) container.ts;
                        Assert.assertTrue(idx < 100 && container.label.equals("foo"));
                        Assert.assertEquals(container.isOut, idx % 2 == 0);
                        Assert.assertEquals(container.tid, "d".getBytes());
                        Assert.assertEquals(container.property, ("b" + idx).getBytes());
                    }
                }
                Assert.assertEquals(count, 100);
                break;
            default:
                throw new RuntimeException("unsupported filter type " + filterType.name());
        }

    }

    private void testMultiPointGetVertexAndEdgeWithPushDown(NativeGraphStore store,
                                                       FilterType filterType) {
        byte[][] keys = new byte[200][];
        for (int i = 0; i < 200; i++) {
            keys[i] = ("a" + i).getBytes();
        }
        byte[] pushDown = generatePushDown(filterType, keys);
        int vertexCount = 0;
        int edgeCount = 0;
        switch (filterType) {
            case EMPTY:
            case VERTEX_MUST_CONTAIN:
            case OR:
                try (CStoreVertexAndEdgeIterator iterator = store.scanVertexAndEdge(keys, pushDown)) {
                    while (iterator.hasNext()) {
                        VertexAndEdgeContainer vertexAndEdge = iterator.next();
                        int idx = Integer.parseInt(new String(vertexAndEdge.sid).substring(1));
                        try (CStoreVertexIterator vertexIterator = new CStoreVertexIterator(vertexAndEdge.vertexIt)) {
                            vertexCount++;
                            Assert.assertTrue(vertexIterator.hasNext());
                            VertexContainer vertex = vertexIterator.next();
                            Assert.assertTrue(idx < 100 && vertex.label.equals("foo") || vertex.label.equals("foo" + idx));
                            Assert.assertEquals(vertex.ts, idx);
                            Assert.assertEquals(vertex.property, ("b" + idx).getBytes());
                        }
                        try (CStoreEdgeIterator edgeIterator = new CStoreEdgeIterator(vertexAndEdge.edgeIt)) {
                            edgeCount++;
                            Assert.assertTrue(edgeIterator.hasNext());
                            EdgeContainer edge = edgeIterator.next();
                            Assert.assertTrue(idx < 100 && edge.label.equals("foo") || edge.label.equals("foo" + idx));
                            Assert.assertEquals(edge.ts, idx);
                            Assert.assertEquals(edge.isOut, idx % 2 == 0);
                            Assert.assertEquals(edge.tid, "d".getBytes());
                            Assert.assertEquals(edge.property, ("b" + idx).getBytes());
                        }
                    }
                    Assert.assertEquals(vertexCount, 200);
                    Assert.assertEquals(edgeCount, 200);
                }
                break;
            case ONLY_VERTEX:
                try (CStoreVertexAndEdgeIterator iterator = store.scanVertexAndEdge(keys, pushDown)) {
                    while (iterator.hasNext()) {
                        VertexAndEdgeContainer vertexAndEdge = iterator.next();
                        int idx = Integer.parseInt(new String(vertexAndEdge.sid).substring(1));
                        try (CStoreVertexIterator vertexIterator = new CStoreVertexIterator(vertexAndEdge.vertexIt)) {
                            vertexCount++;
                            Assert.assertTrue(vertexIterator.hasNext());
                            VertexContainer vertex = vertexIterator.next();
                            Assert.assertTrue(idx < 100 && vertex.label.equals("foo") || vertex.label.equals("foo" + idx));
                            Assert.assertEquals(vertex.ts, idx);
                            Assert.assertEquals(vertex.property, ("b" + idx).getBytes());
                        }
                        try (CStoreEdgeIterator edgeIterator = new CStoreEdgeIterator(vertexAndEdge.edgeIt)) {
                            Assert.assertFalse(edgeIterator.hasNext());
                        }
                    }
                    Assert.assertEquals(vertexCount, 200);
                }
                break;
            case IN_EDGE:
                try (CStoreVertexAndEdgeIterator iterator = store.scanVertexAndEdge(keys, pushDown)) {
                    while (iterator.hasNext()) {
                        VertexAndEdgeContainer vertexAndEdge = iterator.next();
                        int idx = Integer.parseInt(new String(vertexAndEdge.sid).substring(1));
                        try (CStoreVertexIterator vertexIterator = new CStoreVertexIterator(vertexAndEdge.vertexIt)) {
                            vertexCount++;
                            Assert.assertTrue(vertexIterator.hasNext());
                            VertexContainer vertex = vertexIterator.next();
                            Assert.assertTrue(idx < 100 && vertex.label.equals("foo") || vertex.label.equals("foo" + idx));
                            Assert.assertEquals(vertex.ts, idx);
                            Assert.assertEquals(vertex.property, ("b" + idx).getBytes());
                        }
                        try (CStoreEdgeIterator edgeIterator = new CStoreEdgeIterator(vertexAndEdge.edgeIt)) {
                            if (idx % 2 == 1) {
                                edgeCount++;
                                Assert.assertTrue(edgeIterator.hasNext());
                                EdgeContainer edge = edgeIterator.next();
                                Assert.assertTrue(idx < 100 && edge.label.equals("foo") || edge.label.equals("foo" + idx));
                                Assert.assertEquals(edge.ts, idx);
                                Assert.assertFalse(edge.isOut);
                                Assert.assertEquals(edge.tid, "d".getBytes());
                                Assert.assertEquals(edge.property, ("b" + idx).getBytes());
                            } else {
                                Assert.assertFalse(edgeIterator.hasNext());
                            }
                        }
                    }
                    Assert.assertEquals(vertexCount, 200);
                    Assert.assertEquals(edgeCount, 100);
                }
                break;
            case OUT_EDGE:
                try (CStoreVertexAndEdgeIterator iterator = store.scanVertexAndEdge(keys, pushDown)) {
                    while (iterator.hasNext()) {
                        VertexAndEdgeContainer vertexAndEdge = iterator.next();
                        int idx = Integer.parseInt(new String(vertexAndEdge.sid).substring(1));
                        try (CStoreVertexIterator vertexIterator = new CStoreVertexIterator(vertexAndEdge.vertexIt)) {
                            vertexCount++;
                            Assert.assertTrue(vertexIterator.hasNext());
                            VertexContainer vertex = vertexIterator.next();
                            Assert.assertTrue(idx < 100 && vertex.label.equals("foo") || vertex.label.equals("foo" + idx));
                            Assert.assertEquals(vertex.ts, idx);
                            Assert.assertEquals(vertex.property, ("b" + idx).getBytes());
                        }
                        try (CStoreEdgeIterator edgeIterator = new CStoreEdgeIterator(vertexAndEdge.edgeIt)) {
                            if (idx % 2 == 0) {
                                edgeCount++;
                                Assert.assertTrue(edgeIterator.hasNext());
                                EdgeContainer edge = edgeIterator.next();
                                Assert.assertTrue(idx < 100 && edge.label.equals("foo") || edge.label.equals("foo" + idx));
                                Assert.assertEquals(edge.ts, idx);
                                Assert.assertTrue(edge.isOut);
                                Assert.assertEquals(edge.tid, "d".getBytes());
                                Assert.assertEquals(edge.property, ("b" + idx).getBytes());
                            } else {
                                Assert.assertFalse(edgeIterator.hasNext());
                            }
                        }
                    }
                    Assert.assertEquals(vertexCount, 200);
                    Assert.assertEquals(edgeCount, 100);
                }
                break;
            case VERTEX_VALUE_DROP:
                try (CStoreVertexAndEdgeIterator iterator = store.scanVertexAndEdge(keys, pushDown)) {
                    while (iterator.hasNext()) {
                        VertexAndEdgeContainer vertexAndEdge = iterator.next();
                        int idx = Integer.parseInt(new String(vertexAndEdge.sid).substring(1));
                        try (CStoreVertexIterator vertexIterator = new CStoreVertexIterator(vertexAndEdge.vertexIt)) {
                            vertexCount++;
                            Assert.assertTrue(vertexIterator.hasNext());
                            VertexContainer vertex = vertexIterator.next();
                            Assert.assertTrue(idx < 100 && vertex.label.equals("foo") || vertex.label.equals("foo" + idx));
                            Assert.assertEquals(vertex.ts, idx);
                            Assert.assertEquals(vertex.property, new byte[0]);
                        }
                        try (CStoreEdgeIterator edgeIterator = new CStoreEdgeIterator(vertexAndEdge.edgeIt)) {
                            edgeCount++;
                            Assert.assertTrue(edgeIterator.hasNext());
                            EdgeContainer edge = edgeIterator.next();
                            Assert.assertTrue(idx < 100 && edge.label.equals("foo") || edge.label.equals("foo" + idx));
                            Assert.assertEquals(edge.ts, idx);
                            Assert.assertEquals(edge.isOut, idx % 2 == 0);
                            Assert.assertEquals(edge.tid, "d".getBytes());
                            Assert.assertEquals(edge.property, ("b" + idx).getBytes());
                        }
                    }
                    Assert.assertEquals(vertexCount, 200);
                    Assert.assertEquals(edgeCount, 200);
                }
                break;
            case EDGE_VALUE_DROP:
                try (CStoreVertexAndEdgeIterator iterator = store.scanVertexAndEdge(keys, pushDown)) {
                    while (iterator.hasNext()) {
                        VertexAndEdgeContainer vertexAndEdge = iterator.next();
                        int idx = Integer.parseInt(new String(vertexAndEdge.sid).substring(1));
                        try (CStoreVertexIterator vertexIterator = new CStoreVertexIterator(vertexAndEdge.vertexIt)) {
                            vertexCount++;
                            Assert.assertTrue(vertexIterator.hasNext());
                            VertexContainer vertex = vertexIterator.next();
                            Assert.assertTrue(idx < 100 && vertex.label.equals("foo") || vertex.label.equals("foo" + idx));
                            Assert.assertEquals(vertex.ts, idx);
                            Assert.assertEquals(vertex.property, ("b" + idx).getBytes());
                        }
                        try (CStoreEdgeIterator edgeIterator = new CStoreEdgeIterator(vertexAndEdge.edgeIt)) {
                            edgeCount++;
                            Assert.assertTrue(edgeIterator.hasNext());
                            EdgeContainer edge = edgeIterator.next();
                            Assert.assertTrue(idx < 100 && edge.label.equals("foo") || edge.label.equals("foo" + idx));
                            Assert.assertEquals(edge.ts, idx);
                            Assert.assertEquals(edge.isOut, idx % 2 == 0);
                            Assert.assertEquals(edge.tid, "d".getBytes());
                            Assert.assertEquals(edge.property, new byte[0]);
                        }
                    }
                    Assert.assertEquals(vertexCount, 200);
                    Assert.assertEquals(edgeCount, 200);
                }
                break;
            case AND:
                try (CStoreVertexAndEdgeIterator iterator = store.scanVertexAndEdge(keys, pushDown)) {
                    while (iterator.hasNext()) {
                        VertexAndEdgeContainer vertexAndEdge = iterator.next();
                        int idx = 0;
                        try {
                            idx = Integer.parseInt(new String(vertexAndEdge.sid).substring(1));
                        } catch (Throwable t) {
                            System.out.println("XXX");
                        }
                        try (CStoreVertexIterator vertexIterator = new CStoreVertexIterator(vertexAndEdge.vertexIt)) {
                            if (idx < 10) {
                                vertexCount++;
                                Assert.assertTrue(vertexIterator.hasNext());
                                VertexContainer vertex = vertexIterator.next();
                                Assert.assertTrue(idx < 100 && vertex.label.equals("foo") || vertex.label.equals("foo" + idx));
                                Assert.assertEquals(vertex.ts, idx);
                                Assert.assertEquals(vertex.property, ("b" + idx).getBytes());
                            } else {
                                Assert.assertFalse(vertexIterator.hasNext());
                            }
                        }
                        try (CStoreEdgeIterator edgeIterator = new CStoreEdgeIterator(vertexAndEdge.edgeIt)) {
                            if (idx % 2 == 0) {
                                edgeCount++;
                                Assert.assertTrue(edgeIterator.hasNext());
                                EdgeContainer edge = edgeIterator.next();
                                Assert.assertTrue(idx < 100 && edge.label.equals("foo") || edge.label.equals("foo" + idx));
                                Assert.assertEquals(edge.ts, idx);
                                Assert.assertTrue(edge.isOut);
                                Assert.assertEquals(edge.tid, "d".getBytes());
                                Assert.assertEquals(edge.property, ("b" + idx).getBytes());
                            } else {
                                Assert.assertFalse(edgeIterator.hasNext());
                            }
                        }
                    }
                    Assert.assertEquals(vertexCount, 10);
                    Assert.assertEquals(edgeCount, 100);
                }
                break;
            case VERTEX_TS:
                try (CStoreVertexAndEdgeIterator iterator = store.scanVertexAndEdge(keys, pushDown)) {
                    while (iterator.hasNext()) {
                        VertexAndEdgeContainer vertexAndEdge = iterator.next();
                        int idx = Integer.parseInt(new String(vertexAndEdge.sid).substring(1));
                        try (CStoreVertexIterator vertexIterator = new CStoreVertexIterator(vertexAndEdge.vertexIt)) {
                            if (idx < 10) {
                                vertexCount++;
                                Assert.assertTrue(vertexIterator.hasNext());
                                VertexContainer vertex = vertexIterator.next();
                                Assert.assertEquals(vertex.label, "foo");
                                Assert.assertEquals(vertex.ts, idx);
                                Assert.assertEquals(vertex.property, ("b" + idx).getBytes());
                            } else {
                                Assert.assertFalse(vertexIterator.hasNext());
                            }
                        }
                        try (CStoreEdgeIterator edgeIterator = new CStoreEdgeIterator(vertexAndEdge.edgeIt)) {
                            edgeCount++;
                            Assert.assertTrue(edgeIterator.hasNext());
                            EdgeContainer edge = edgeIterator.next();
                            Assert.assertTrue(idx < 100 && edge.label.equals("foo") || edge.label.equals("foo" + idx));
                            Assert.assertEquals(edge.ts, idx);
                            Assert.assertEquals(edge.isOut, idx % 2 == 0);
                            Assert.assertEquals(edge.tid, "d".getBytes());
                            Assert.assertEquals(edge.property, ("b" + idx).getBytes());
                        }
                    }
                    Assert.assertEquals(vertexCount, 10);
                    Assert.assertEquals(edgeCount, 200);
                }
                break;
            case EDGE_TS:
                try (CStoreVertexAndEdgeIterator iterator = store.scanVertexAndEdge(keys, pushDown)) {
                    while (iterator.hasNext()) {
                        VertexAndEdgeContainer vertexAndEdge = iterator.next();
                        int idx = Integer.parseInt(new String(vertexAndEdge.sid).substring(1));
                        try (CStoreVertexIterator vertexIterator = new CStoreVertexIterator(vertexAndEdge.vertexIt)) {
                            vertexCount++;
                            Assert.assertTrue(vertexIterator.hasNext());
                            VertexContainer vertex = vertexIterator.next();
                            Assert.assertTrue(idx < 100 && vertex.label.equals("foo") || vertex.label.equals("foo" + idx));
                            Assert.assertEquals(vertex.ts, idx);
                            Assert.assertEquals(vertex.property, ("b" + idx).getBytes());
                        }
                        try (CStoreEdgeIterator edgeIterator = new CStoreEdgeIterator(vertexAndEdge.edgeIt)) {
                            if (idx < 10) {
                                edgeCount++;
                                Assert.assertTrue(edgeIterator.hasNext());
                                EdgeContainer edge = edgeIterator.next();
                                Assert.assertEquals(edge.label, "foo");
                                Assert.assertEquals(edge.ts, idx);
                                Assert.assertEquals(edge.isOut, idx % 2 == 0);
                                Assert.assertEquals(edge.tid, "d".getBytes());
                                Assert.assertEquals(edge.property, ("b" + idx).getBytes());
                            } else {
                                Assert.assertFalse(edgeIterator.hasNext());
                            }
                        }
                    }
                    Assert.assertEquals(vertexCount, 200);
                    Assert.assertEquals(edgeCount, 10);
                }
                break;
            case MULTI_EDGE_TS:
                try (CStoreVertexAndEdgeIterator iterator = store.scanVertexAndEdge(keys, pushDown)) {
                    while (iterator.hasNext()) {
                        VertexAndEdgeContainer vertexAndEdge = iterator.next();
                        int idx = Integer.parseInt(new String(vertexAndEdge.sid).substring(1));
                        try (CStoreVertexIterator vertexIterator = new CStoreVertexIterator(vertexAndEdge.vertexIt)) {
                            vertexCount++;
                            Assert.assertTrue(vertexIterator.hasNext());
                            VertexContainer vertex = vertexIterator.next();
                            Assert.assertTrue(idx < 100 && vertex.label.equals("foo") || vertex.label.equals("foo" + idx));
                            Assert.assertEquals(vertex.ts, idx);
                            Assert.assertEquals(vertex.property, ("b" + idx).getBytes());
                        }
                        try (CStoreEdgeIterator edgeIterator = new CStoreEdgeIterator(vertexAndEdge.edgeIt)) {
                            if (idx < 20) {
                                edgeCount++;
                                Assert.assertTrue(edgeIterator.hasNext());
                                EdgeContainer edge = edgeIterator.next();
                                Assert.assertEquals(edge.label, "foo");
                                Assert.assertEquals(edge.ts, idx);
                                Assert.assertEquals(edge.isOut, idx % 2 == 0);
                                Assert.assertEquals(edge.tid, "d".getBytes());
                                Assert.assertEquals(edge.property, ("b" + idx).getBytes());
                            } else {
                                Assert.assertFalse(edgeIterator.hasNext());
                            }
                        }
                    }
                    Assert.assertEquals(vertexCount, 200);
                    Assert.assertEquals(edgeCount, 20);
                }
                break;
            case VERTEX_LABEL:
                try (CStoreVertexAndEdgeIterator iterator = store.scanVertexAndEdge(keys, pushDown)) {
                    while (iterator.hasNext()) {
                        VertexAndEdgeContainer vertexAndEdge = iterator.next();
                        int idx = Integer.parseInt(new String(vertexAndEdge.sid).substring(1));
                        try (CStoreVertexIterator vertexIterator = new CStoreVertexIterator(vertexAndEdge.vertexIt)) {
                            if (idx < 100) {
                                vertexCount++;
                                Assert.assertTrue(vertexIterator.hasNext());
                                VertexContainer vertex = vertexIterator.next();
                                Assert.assertEquals(vertex.label, "foo");
                                Assert.assertEquals(vertex.ts, idx);
                                Assert.assertEquals(vertex.property, ("b" + idx).getBytes());
                            } else {
                                Assert.assertFalse(vertexIterator.hasNext());
                            }
                        }
                        try (CStoreEdgeIterator edgeIterator = new CStoreEdgeIterator(vertexAndEdge.edgeIt)) {
                            edgeCount++;
                            Assert.assertTrue(edgeIterator.hasNext());
                            EdgeContainer edge = edgeIterator.next();
                            Assert.assertTrue(idx < 100 && edge.label.equals("foo") || edge.label.equals("foo" + idx));
                            Assert.assertEquals(edge.ts, idx);
                            Assert.assertEquals(edge.isOut, idx % 2 == 0);
                            Assert.assertEquals(edge.tid, "d".getBytes());
                            Assert.assertEquals(edge.property, ("b" + idx).getBytes());
                        }
                    }
                    Assert.assertEquals(vertexCount, 100);
                    Assert.assertEquals(edgeCount, 200);
                }
                break;
            case EDGE_LABEL:
                try (CStoreVertexAndEdgeIterator iterator = store.scanVertexAndEdge(keys, pushDown)) {
                    while (iterator.hasNext()) {
                        VertexAndEdgeContainer vertexAndEdge = iterator.next();
                        int idx = Integer.parseInt(new String(vertexAndEdge.sid).substring(1));
                        try (CStoreVertexIterator vertexIterator = new CStoreVertexIterator(vertexAndEdge.vertexIt)) {
                            vertexCount++;
                            Assert.assertTrue(vertexIterator.hasNext());
                            VertexContainer vertex = vertexIterator.next();
                            Assert.assertTrue(idx < 100 && vertex.label.equals("foo") || vertex.label.equals("foo" + idx));
                            Assert.assertEquals(vertex.ts, idx);
                            Assert.assertEquals(vertex.property, ("b" + idx).getBytes());
                        }
                        try (CStoreEdgeIterator edgeIterator = new CStoreEdgeIterator(vertexAndEdge.edgeIt)) {
                            if (idx < 100) {
                                edgeCount++;
                                Assert.assertTrue(edgeIterator.hasNext());
                                EdgeContainer edge = edgeIterator.next();
                                Assert.assertEquals(edge.label, "foo");
                                Assert.assertEquals(edge.ts, idx);
                                Assert.assertEquals(edge.isOut, idx % 2 == 0);
                                Assert.assertEquals(edge.tid, "d".getBytes());
                                Assert.assertEquals(edge.property, ("b" + idx).getBytes());
                            } else {
                                Assert.assertFalse(edgeIterator.hasNext());
                            }
                        }
                    }
                    Assert.assertEquals(vertexCount, 200);
                    Assert.assertEquals(edgeCount, 100);
                }
                break;
            default:
                throw new RuntimeException("unsupported filter type " + filterType.name());
        }
    }

    private void testScanVertexWithPushDown(NativeGraphStore store, FilterType filterType) {
        byte[] pushDown = generatePushDown(filterType, null);
        int count = 0;
        switch (filterType) {
            case EMPTY:
            case ONLY_VERTEX:
            case OR:
                try (CStoreVertexIterator vertexIterator = store.scanVertex(pushDown)) {
                    while (vertexIterator.hasNext()) {
                        count++;
                        VertexContainer container = vertexIterator.next();
                        int idx = (int) container.ts;
                        Assert.assertTrue(
                            idx < 100 && container.label.equals("foo") || container.label.equals(
                                "foo" + idx));
                        Assert.assertEquals(container.property, ("b" + idx).getBytes());
                    }
                }
                Assert.assertEquals(count, 200);
                break;
            case VERTEX_VALUE_DROP:
                try (CStoreVertexIterator vertexIterator = store.scanVertex(pushDown)) {
                    while (vertexIterator.hasNext()) {
                        count++;
                        VertexContainer container = vertexIterator.next();
                        int idx = (int) container.ts;
                        Assert.assertTrue(
                            idx < 100 && container.label.equals("foo") || container.label.equals(
                                "foo" + idx));
                        Assert.assertEquals(container.property, new byte[0]);
                    }
                }
                Assert.assertEquals(count, 200);
                break;
            case AND:
            case VERTEX_TS:
                try (CStoreVertexIterator vertexIterator = store.scanVertex(pushDown)) {
                    while (vertexIterator.hasNext()) {
                        count++;
                        VertexContainer container = vertexIterator.next();
                        int idx = (int) container.ts;
                        Assert.assertTrue(idx >= 0 && idx < 10);
                        Assert.assertEquals(container.label, "foo");
                        Assert.assertEquals(container.property, ("b" + idx).getBytes());
                    }
                }
                Assert.assertEquals(count, 10);
                break;
            case VERTEX_LABEL:
                try (CStoreVertexIterator vertexIterator = store.scanVertex(pushDown)) {
                    while (vertexIterator.hasNext()) {
                        count++;
                        VertexContainer container = vertexIterator.next();
                        int idx = (int) container.ts;
                        Assert.assertTrue(idx >= 0 && idx < 100);
                        Assert.assertEquals(container.label, "foo");
                        Assert.assertEquals(container.property, ("b" + idx).getBytes());
                    }
                }
                Assert.assertEquals(count, 100);
                break;
            default:
                throw new RuntimeException("unsupported filter type " + filterType.name());
        }
    }

    private void testScanEdgesWithPushDown(NativeGraphStore store, FilterType filterType) {
        byte[] pushDown = generatePushDown(filterType, null);
        int count = 0;
        switch (filterType) {
            case EMPTY:
            case OR:
                try (CStoreEdgeIterator edgeIterator = store.scanEdges(pushDown)) {
                    while (edgeIterator.hasNext()) {
                        count++;
                        EdgeContainer container = edgeIterator.next();
                        int idx = (int) container.ts;
                        Assert.assertTrue(idx < 100 && container.label.equals("foo") || container.label.equals("foo" + idx));
                        Assert.assertEquals(container.isOut, idx % 2 == 0);
                        Assert.assertEquals(container.tid, "d".getBytes());
                        Assert.assertEquals(container.property, ("b" + idx).getBytes());
                    }
                }
                Assert.assertEquals(count, 200);
                break;
            case IN_EDGE:
                try (CStoreEdgeIterator edgeIterator = store.scanEdges(pushDown)) {
                    while (edgeIterator.hasNext()) {
                        count++;
                        EdgeContainer container = edgeIterator.next();
                        int idx = (int) container.ts;
                        Assert.assertTrue(idx < 100 && container.label.equals("foo") || container.label.equals("foo" + idx));
                        Assert.assertFalse(container.isOut);
                        Assert.assertEquals(container.tid, "d".getBytes());
                        Assert.assertEquals(container.property, ("b" + idx).getBytes());
                    }
                }
                Assert.assertEquals(count, 100);
                break;
            case OUT_EDGE:
                try (CStoreEdgeIterator edgeIterator = store.scanEdges(pushDown)) {
                    while (edgeIterator.hasNext()) {
                        count++;
                        EdgeContainer container = edgeIterator.next();
                        int idx = (int) container.ts;
                        Assert.assertTrue(idx < 100 && container.label.equals("foo") || container.label.equals("foo" + idx));
                        Assert.assertTrue(container.isOut);
                        Assert.assertEquals(container.tid, "d".getBytes());
                        Assert.assertEquals(container.property, ("b" + idx).getBytes());
                    }
                }
                Assert.assertEquals(count, 100);
                break;
            case AND:
                try (CStoreEdgeIterator edgeIterator = store.scanEdges(pushDown)) {
                    while (edgeIterator.hasNext()) {
                        count++;
                        EdgeContainer container = edgeIterator.next();
                        int idx = (int) container.ts;
                        Assert.assertTrue(idx < 100 && container.label.equals("foo") || container.label.equals("foo" + idx));
                        Assert.assertTrue(container.isOut);
                        Assert.assertEquals(container.tid, "d".getBytes());
                        Assert.assertEquals(container.property, ("b" + idx).getBytes());
                    }
                }
                Assert.assertEquals(count, 100);
                break;
            case MULTI_EDGE_TS:
                try (CStoreEdgeIterator edgeIterator = store.scanEdges(pushDown)) {
                    while (edgeIterator.hasNext()) {
                        count++;
                        EdgeContainer container = edgeIterator.next();
                        int idx = (int) container.ts;
                        Assert.assertTrue(idx < 20 && container.label.equals("foo"));
                        Assert.assertEquals(container.isOut, idx % 2 == 0);
                        Assert.assertEquals(container.tid, "d".getBytes());
                        Assert.assertEquals(container.property, ("b" + idx).getBytes());
                    }
                }
                Assert.assertEquals(count, 20);
                break;
            case EDGE_VALUE_DROP:
                try (CStoreEdgeIterator edgeIterator = store.scanEdges(pushDown)) {
                    while (edgeIterator.hasNext()) {
                        count++;
                        EdgeContainer container = edgeIterator.next();
                        int idx = (int) container.ts;
                        Assert.assertTrue(idx < 100 && container.label.equals("foo") || container.label.equals("foo" + idx));
                        Assert.assertEquals(container.isOut, idx % 2 == 0);
                        Assert.assertEquals(container.tid, "d".getBytes());
                        Assert.assertEquals(container.property, new byte[0]);
                    }
                }
                Assert.assertEquals(count, 200);
                break;
            case EDGE_TS:
                try (CStoreEdgeIterator edgeIterator = store.scanEdges(pushDown)) {
                    while (edgeIterator.hasNext()) {
                        count++;
                        EdgeContainer container = edgeIterator.next();
                        int idx = (int) container.ts;
                        Assert.assertTrue(idx < 10 && container.label.equals("foo"));
                        Assert.assertEquals(container.isOut, idx % 2 == 0);
                        Assert.assertEquals(container.tid, "d".getBytes());
                        Assert.assertEquals(container.property, ("b" + idx).getBytes());
                    }
                }
                Assert.assertEquals(count, 10);
                break;
            case EDGE_LABEL:
                try (CStoreEdgeIterator edgeIterator = store.scanEdges(pushDown)) {
                    while (edgeIterator.hasNext()) {
                        count++;
                        EdgeContainer container = edgeIterator.next();
                        int idx = (int) container.ts;
                        Assert.assertTrue(idx < 100 && container.label.equals("foo"));
                        Assert.assertEquals(container.isOut, idx % 2 == 0);
                        Assert.assertEquals(container.tid, "d".getBytes());
                        Assert.assertEquals(container.property, ("b" + idx).getBytes());
                    }
                }
                Assert.assertEquals(count, 100);
                break;
            default:
                throw new RuntimeException("unsupported filter type " + filterType.name());
        }

    }

    private void testScanVertexAndEdgeWithPushDown(NativeGraphStore store, FilterType filterType) {
        byte[] pushDown = generatePushDown(filterType, null);
        int vertexCount = 0;
        int edgeCount = 0;
        switch (filterType) {
            case EMPTY:
            case VERTEX_MUST_CONTAIN:
            case OR:
                try (CStoreVertexAndEdgeIterator iterator = store.scanVertexAndEdge(
                    pushDown)) {
                    while (iterator.hasNext()) {
                        VertexAndEdgeContainer vertexAndEdge = iterator.next();
                        int idx = Integer.parseInt(new String(vertexAndEdge.sid).substring(1));
                        try (CStoreVertexIterator vertexIterator = new CStoreVertexIterator(
                            vertexAndEdge.vertexIt)) {
                            vertexCount++;
                            Assert.assertTrue(vertexIterator.hasNext());
                            VertexContainer vertex = vertexIterator.next();
                            Assert.assertTrue(
                                idx < 100 && vertex.label.equals("foo") || vertex.label.equals(
                                    "foo" + idx));
                            Assert.assertEquals(vertex.ts, idx);
                            Assert.assertEquals(vertex.property, ("b" + idx).getBytes());
                        }
                        try (CStoreEdgeIterator edgeIterator = new CStoreEdgeIterator(
                            vertexAndEdge.edgeIt)) {
                            edgeCount++;
                            Assert.assertTrue(edgeIterator.hasNext());
                            EdgeContainer edge = edgeIterator.next();
                            Assert.assertTrue(
                                idx < 100 && edge.label.equals("foo") || edge.label.equals(
                                    "foo" + idx));
                            Assert.assertEquals(edge.ts, idx);
                            Assert.assertEquals(edge.isOut, idx % 2 == 0);
                            Assert.assertEquals(edge.tid, "d".getBytes());
                            Assert.assertEquals(edge.property, ("b" + idx).getBytes());
                        }
                    }
                    Assert.assertEquals(vertexCount, 200);
                    Assert.assertEquals(edgeCount, 200);
                }
                break;
            case ONLY_VERTEX:
                try (CStoreVertexAndEdgeIterator iterator = store.scanVertexAndEdge(
                    pushDown)) {
                    while (iterator.hasNext()) {
                        VertexAndEdgeContainer vertexAndEdge = iterator.next();
                        int idx = Integer.parseInt(new String(vertexAndEdge.sid).substring(1));
                        try (CStoreVertexIterator vertexIterator = new CStoreVertexIterator(
                            vertexAndEdge.vertexIt)) {
                            vertexCount++;
                            Assert.assertTrue(vertexIterator.hasNext());
                            VertexContainer vertex = vertexIterator.next();
                            Assert.assertTrue(
                                idx < 100 && vertex.label.equals("foo") || vertex.label.equals(
                                    "foo" + idx));
                            Assert.assertEquals(vertex.ts, idx);
                            Assert.assertEquals(vertex.property, ("b" + idx).getBytes());
                        }
                        try (CStoreEdgeIterator edgeIterator = new CStoreEdgeIterator(
                            vertexAndEdge.edgeIt)) {
                            Assert.assertFalse(edgeIterator.hasNext());
                        }
                    }
                    Assert.assertEquals(vertexCount, 200);
                }
                break;
            case IN_EDGE:
                try (CStoreVertexAndEdgeIterator iterator = store.scanVertexAndEdge(
                    pushDown)) {
                    while (iterator.hasNext()) {
                        VertexAndEdgeContainer vertexAndEdge = iterator.next();
                        int idx = Integer.parseInt(new String(vertexAndEdge.sid).substring(1));
                        try (CStoreVertexIterator vertexIterator = new CStoreVertexIterator(
                            vertexAndEdge.vertexIt)) {
                            vertexCount++;
                            Assert.assertTrue(vertexIterator.hasNext());
                            VertexContainer vertex = vertexIterator.next();
                            Assert.assertTrue(
                                idx < 100 && vertex.label.equals("foo") || vertex.label.equals(
                                    "foo" + idx));
                            Assert.assertEquals(vertex.ts, idx);
                            Assert.assertEquals(vertex.property, ("b" + idx).getBytes());
                        }
                        try (CStoreEdgeIterator edgeIterator = new CStoreEdgeIterator(
                            vertexAndEdge.edgeIt)) {
                            if (idx % 2 == 1) {
                                edgeCount++;
                                Assert.assertTrue(edgeIterator.hasNext());
                                EdgeContainer edge = edgeIterator.next();
                                Assert.assertTrue(
                                    idx < 100 && edge.label.equals("foo") || edge.label.equals(
                                        "foo" + idx));
                                Assert.assertEquals(edge.ts, idx);
                                Assert.assertFalse(edge.isOut);
                                Assert.assertEquals(edge.tid, "d".getBytes());
                                Assert.assertEquals(edge.property, ("b" + idx).getBytes());
                            } else {
                                Assert.assertFalse(edgeIterator.hasNext());
                            }
                        }
                    }
                    Assert.assertEquals(vertexCount, 200);
                    Assert.assertEquals(edgeCount, 100);
                }
                break;
            case OUT_EDGE:
                try (CStoreVertexAndEdgeIterator iterator = store.scanVertexAndEdge(
                    pushDown)) {
                    while (iterator.hasNext()) {
                        VertexAndEdgeContainer vertexAndEdge = iterator.next();
                        int idx = Integer.parseInt(new String(vertexAndEdge.sid).substring(1));
                        try (CStoreVertexIterator vertexIterator = new CStoreVertexIterator(
                            vertexAndEdge.vertexIt)) {
                            vertexCount++;
                            Assert.assertTrue(vertexIterator.hasNext());
                            VertexContainer vertex = vertexIterator.next();
                            Assert.assertTrue(
                                idx < 100 && vertex.label.equals("foo") || vertex.label.equals(
                                    "foo" + idx));
                            Assert.assertEquals(vertex.ts, idx);
                            Assert.assertEquals(vertex.property, ("b" + idx).getBytes());
                        }
                        try (CStoreEdgeIterator edgeIterator = new CStoreEdgeIterator(
                            vertexAndEdge.edgeIt)) {
                            if (idx % 2 == 0) {
                                edgeCount++;
                                Assert.assertTrue(edgeIterator.hasNext());
                                EdgeContainer edge = edgeIterator.next();
                                Assert.assertTrue(
                                    idx < 100 && edge.label.equals("foo") || edge.label.equals(
                                        "foo" + idx));
                                Assert.assertEquals(edge.ts, idx);
                                Assert.assertTrue(edge.isOut);
                                Assert.assertEquals(edge.tid, "d".getBytes());
                                Assert.assertEquals(edge.property, ("b" + idx).getBytes());
                            } else {
                                Assert.assertFalse(edgeIterator.hasNext());
                            }
                        }
                    }
                    Assert.assertEquals(vertexCount, 200);
                    Assert.assertEquals(edgeCount, 100);
                }
                break;
            case VERTEX_VALUE_DROP:
                try (CStoreVertexAndEdgeIterator iterator = store.scanVertexAndEdge(
                    pushDown)) {
                    while (iterator.hasNext()) {
                        VertexAndEdgeContainer vertexAndEdge = iterator.next();
                        int idx = Integer.parseInt(new String(vertexAndEdge.sid).substring(1));
                        try (CStoreVertexIterator vertexIterator = new CStoreVertexIterator(
                            vertexAndEdge.vertexIt)) {
                            vertexCount++;
                            Assert.assertTrue(vertexIterator.hasNext());
                            VertexContainer vertex = vertexIterator.next();
                            Assert.assertTrue(
                                idx < 100 && vertex.label.equals("foo") || vertex.label.equals(
                                    "foo" + idx));
                            Assert.assertEquals(vertex.ts, idx);
                            Assert.assertEquals(vertex.property, new byte[0]);
                        }
                        try (CStoreEdgeIterator edgeIterator = new CStoreEdgeIterator(
                            vertexAndEdge.edgeIt)) {
                            edgeCount++;
                            Assert.assertTrue(edgeIterator.hasNext());
                            EdgeContainer edge = edgeIterator.next();
                            Assert.assertTrue(
                                idx < 100 && edge.label.equals("foo") || edge.label.equals(
                                    "foo" + idx));
                            Assert.assertEquals(edge.ts, idx);
                            Assert.assertEquals(edge.isOut, idx % 2 == 0);
                            Assert.assertEquals(edge.tid, "d".getBytes());
                            Assert.assertEquals(edge.property, ("b" + idx).getBytes());
                        }
                    }
                    Assert.assertEquals(vertexCount, 200);
                    Assert.assertEquals(edgeCount, 200);
                }
                break;
            case EDGE_VALUE_DROP:
                try (CStoreVertexAndEdgeIterator iterator = store.scanVertexAndEdge(
                    pushDown)) {
                    while (iterator.hasNext()) {
                        VertexAndEdgeContainer vertexAndEdge = iterator.next();
                        int idx = Integer.parseInt(new String(vertexAndEdge.sid).substring(1));
                        try (CStoreVertexIterator vertexIterator = new CStoreVertexIterator(
                            vertexAndEdge.vertexIt)) {
                            vertexCount++;
                            Assert.assertTrue(vertexIterator.hasNext());
                            VertexContainer vertex = vertexIterator.next();
                            Assert.assertTrue(
                                idx < 100 && vertex.label.equals("foo") || vertex.label.equals(
                                    "foo" + idx));
                            Assert.assertEquals(vertex.ts, idx);
                            Assert.assertEquals(vertex.property, ("b" + idx).getBytes());
                        }
                        try (CStoreEdgeIterator edgeIterator = new CStoreEdgeIterator(
                            vertexAndEdge.edgeIt)) {
                            edgeCount++;
                            Assert.assertTrue(edgeIterator.hasNext());
                            EdgeContainer edge = edgeIterator.next();
                            Assert.assertTrue(
                                idx < 100 && edge.label.equals("foo") || edge.label.equals(
                                    "foo" + idx));
                            Assert.assertEquals(edge.ts, idx);
                            Assert.assertEquals(edge.isOut, idx % 2 == 0);
                            Assert.assertEquals(edge.tid, "d".getBytes());
                            Assert.assertEquals(edge.property, new byte[0]);
                        }
                    }
                    Assert.assertEquals(vertexCount, 200);
                    Assert.assertEquals(edgeCount, 200);
                }
                break;
            case AND:
                try (CStoreVertexAndEdgeIterator iterator = store.scanVertexAndEdge(
                    pushDown)) {
                    while (iterator.hasNext()) {
                        VertexAndEdgeContainer vertexAndEdge = iterator.next();
                        int idx = Integer.parseInt(new String(vertexAndEdge.sid).substring(1));
                        try (CStoreVertexIterator vertexIterator = new CStoreVertexIterator(
                            vertexAndEdge.vertexIt)) {
                            if (idx < 10) {
                                vertexCount++;
                                Assert.assertTrue(vertexIterator.hasNext());
                                VertexContainer vertex = vertexIterator.next();
                                Assert.assertTrue(vertex.label.equals("foo") || vertex.label.equals("foo" + idx));
                                Assert.assertEquals(vertex.ts, idx);
                                Assert.assertEquals(vertex.property, ("b" + idx).getBytes());
                            } else {
                                Assert.assertFalse(vertexIterator.hasNext());
                            }
                        }
                        try (CStoreEdgeIterator edgeIterator = new CStoreEdgeIterator(
                            vertexAndEdge.edgeIt)) {
                            if (idx % 2 == 0) {
                                edgeCount++;
                                Assert.assertTrue(edgeIterator.hasNext());
                                EdgeContainer edge = edgeIterator.next();
                                Assert.assertTrue(
                                    idx < 100 && edge.label.equals("foo") || edge.label.equals(
                                        "foo" + idx));
                                Assert.assertEquals(edge.ts, idx);
                                Assert.assertTrue(edge.isOut);
                                Assert.assertEquals(edge.tid, "d".getBytes());
                                Assert.assertEquals(edge.property, ("b" + idx).getBytes());
                            } else {
                                Assert.assertFalse(edgeIterator.hasNext());
                            }
                        }
                    }
                    Assert.assertEquals(vertexCount, 10);
                    Assert.assertEquals(edgeCount, 100);
                }
                break;
            case VERTEX_TS:
                try (CStoreVertexAndEdgeIterator iterator = store.scanVertexAndEdge(
                    pushDown)) {
                    while (iterator.hasNext()) {
                        VertexAndEdgeContainer vertexAndEdge = iterator.next();
                        int idx = Integer.parseInt(new String(vertexAndEdge.sid).substring(1));
                        try (CStoreVertexIterator vertexIterator = new CStoreVertexIterator(
                            vertexAndEdge.vertexIt)) {
                            if (idx < 10) {
                                vertexCount++;
                                Assert.assertTrue(vertexIterator.hasNext());
                                VertexContainer vertex = vertexIterator.next();
                                Assert.assertTrue(vertex.label.equals("foo"));
                                Assert.assertEquals(vertex.ts, idx);
                                Assert.assertEquals(vertex.property, ("b" + idx).getBytes());
                            } else {
                                Assert.assertFalse(vertexIterator.hasNext());
                            }
                        }
                        try (CStoreEdgeIterator edgeIterator = new CStoreEdgeIterator(
                            vertexAndEdge.edgeIt)) {
                            edgeCount++;
                            Assert.assertTrue(edgeIterator.hasNext());
                            EdgeContainer edge = edgeIterator.next();
                            Assert.assertTrue(
                                idx < 100 && edge.label.equals("foo") || edge.label.equals(
                                    "foo" + idx));
                            Assert.assertEquals(edge.ts, idx);
                            Assert.assertEquals(edge.isOut, idx % 2 == 0);
                            Assert.assertEquals(edge.tid, "d".getBytes());
                            Assert.assertEquals(edge.property, ("b" + idx).getBytes());
                        }
                    }
                    Assert.assertEquals(vertexCount, 10);
                    Assert.assertEquals(edgeCount, 200);
                }
                break;
            case EDGE_TS:
                try (CStoreVertexAndEdgeIterator iterator = store.scanVertexAndEdge(
                    pushDown)) {
                    while (iterator.hasNext()) {
                        VertexAndEdgeContainer vertexAndEdge = iterator.next();
                        int idx = Integer.parseInt(new String(vertexAndEdge.sid).substring(1));
                        try (CStoreVertexIterator vertexIterator = new CStoreVertexIterator(
                            vertexAndEdge.vertexIt)) {
                            vertexCount++;
                            Assert.assertTrue(vertexIterator.hasNext());
                            VertexContainer vertex = vertexIterator.next();
                            Assert.assertTrue(
                                idx < 100 && vertex.label.equals("foo") || vertex.label.equals(
                                    "foo" + idx));
                            Assert.assertEquals(vertex.ts, idx);
                            Assert.assertEquals(vertex.property, ("b" + idx).getBytes());
                        }
                        try (CStoreEdgeIterator edgeIterator = new CStoreEdgeIterator(
                            vertexAndEdge.edgeIt)) {
                            if (idx < 10) {
                                edgeCount++;
                                Assert.assertTrue(edgeIterator.hasNext());
                                EdgeContainer edge = edgeIterator.next();
                                Assert.assertEquals(edge.label, "foo");
                                Assert.assertEquals(edge.ts, idx);
                                Assert.assertEquals(edge.isOut, idx % 2 == 0);
                                Assert.assertEquals(edge.tid, "d".getBytes());
                                Assert.assertEquals(edge.property, ("b" + idx).getBytes());
                            } else {
                                Assert.assertFalse(edgeIterator.hasNext());
                            }
                        }
                    }
                    Assert.assertEquals(vertexCount, 200);
                    Assert.assertEquals(edgeCount, 10);
                }
                break;
            case MULTI_EDGE_TS:
                try (CStoreVertexAndEdgeIterator iterator = store.scanVertexAndEdge(
                    pushDown)) {
                    while (iterator.hasNext()) {
                        VertexAndEdgeContainer vertexAndEdge = iterator.next();
                        int idx = Integer.parseInt(new String(vertexAndEdge.sid).substring(1));
                        try (CStoreVertexIterator vertexIterator = new CStoreVertexIterator(
                            vertexAndEdge.vertexIt)) {
                            vertexCount++;
                            Assert.assertTrue(vertexIterator.hasNext());
                            VertexContainer vertex = vertexIterator.next();
                            Assert.assertTrue(
                                idx < 100 && vertex.label.equals("foo") || vertex.label.equals(
                                    "foo" + idx));
                            Assert.assertEquals(vertex.ts, idx);
                            Assert.assertEquals(vertex.property, ("b" + idx).getBytes());
                        }
                        try (CStoreEdgeIterator edgeIterator = new CStoreEdgeIterator(
                            vertexAndEdge.edgeIt)) {
                            if (idx < 20) {
                                edgeCount++;
                                Assert.assertTrue(edgeIterator.hasNext());
                                EdgeContainer edge = edgeIterator.next();
                                Assert.assertTrue(edge.label.equals("foo"));
                                Assert.assertEquals(edge.ts, idx);
                                Assert.assertEquals(edge.isOut, idx % 2 == 0);
                                Assert.assertEquals(edge.tid, "d".getBytes());
                                Assert.assertEquals(edge.property, ("b" + idx).getBytes());
                            } else {
                                Assert.assertFalse(edgeIterator.hasNext());
                            }
                        }
                    }
                    Assert.assertEquals(vertexCount, 200);
                    Assert.assertEquals(edgeCount, 20);
                }
                break;
            case VERTEX_LABEL:
                try (CStoreVertexAndEdgeIterator iterator = store.scanVertexAndEdge(
                    pushDown)) {
                    while (iterator.hasNext()) {
                        VertexAndEdgeContainer vertexAndEdge = iterator.next();
                        int idx = Integer.parseInt(new String(vertexAndEdge.sid).substring(1));
                        try (CStoreVertexIterator vertexIterator = new CStoreVertexIterator(
                            vertexAndEdge.vertexIt)) {
                            if (idx < 100) {
                                vertexCount++;
                                Assert.assertTrue(vertexIterator.hasNext());
                                VertexContainer vertex = vertexIterator.next();
                                Assert.assertEquals(vertex.label, "foo");
                                Assert.assertEquals(vertex.ts, idx);
                                Assert.assertEquals(vertex.property, ("b" + idx).getBytes());
                            } else {
                                Assert.assertFalse(vertexIterator.hasNext());
                            }
                        }
                        try (CStoreEdgeIterator edgeIterator = new CStoreEdgeIterator(
                            vertexAndEdge.edgeIt)) {
                            edgeCount++;
                            Assert.assertTrue(edgeIterator.hasNext());
                            EdgeContainer edge = edgeIterator.next();
                            Assert.assertTrue(
                                idx < 100 && edge.label.equals("foo") || edge.label.equals(
                                    "foo" + idx));
                            Assert.assertEquals(edge.ts, idx);
                            Assert.assertEquals(edge.isOut, idx % 2 == 0);
                            Assert.assertEquals(edge.tid, "d".getBytes());
                            Assert.assertEquals(edge.property, ("b" + idx).getBytes());
                        }
                    }
                    Assert.assertEquals(vertexCount, 100);
                    Assert.assertEquals(edgeCount, 200);
                }
                break;
            case EDGE_LABEL:
                try (CStoreVertexAndEdgeIterator iterator = store.scanVertexAndEdge(
                    pushDown)) {
                    while (iterator.hasNext()) {
                        VertexAndEdgeContainer vertexAndEdge = iterator.next();
                        int idx = Integer.parseInt(new String(vertexAndEdge.sid).substring(1));
                        try (CStoreVertexIterator vertexIterator = new CStoreVertexIterator(
                            vertexAndEdge.vertexIt)) {
                            vertexCount++;
                            Assert.assertTrue(vertexIterator.hasNext());
                            VertexContainer vertex = vertexIterator.next();
                            Assert.assertTrue(
                                idx < 100 && vertex.label.equals("foo") || vertex.label.equals(
                                    "foo" + idx));
                            Assert.assertEquals(vertex.ts, idx);
                            Assert.assertEquals(vertex.property, ("b" + idx).getBytes());
                        }
                        try (CStoreEdgeIterator edgeIterator = new CStoreEdgeIterator(
                            vertexAndEdge.edgeIt)) {
                            if (idx < 100) {
                                edgeCount++;
                                Assert.assertTrue(edgeIterator.hasNext());
                                EdgeContainer edge = edgeIterator.next();
                                Assert.assertEquals(edge.label, "foo");
                                Assert.assertEquals(edge.ts, idx);
                                Assert.assertEquals(edge.isOut, idx % 2 == 0);
                                Assert.assertEquals(edge.tid, "d".getBytes());
                                Assert.assertEquals(edge.property, ("b" + idx).getBytes());
                            } else {
                                Assert.assertFalse(edgeIterator.hasNext());
                            }
                        }
                    }
                    Assert.assertEquals(vertexCount, 200);
                    Assert.assertEquals(edgeCount, 100);
                }
                break;
            default:
                throw new RuntimeException("unsupported filter type " + filterType.name());
        }
    }

    private byte[] generateLimitPushDown(long out, long in) {
        Builder pushDownBuilder = PushDown.newBuilder();
        pushDownBuilder.setEdgeLimit(EdgeLimit.newBuilder().setIn(in).setOut(out).setIsSingle(true).build());

        return pushDownBuilder.build().toByteArray();
    }

    private byte[] generatePushDown(FilterType filterType, byte[][] keys) {
        FilterNode filterNode = generateFilterNode(filterType);
        Builder pushDownBuilder = PushDown.newBuilder();
        if (keys == null) {
            pushDownBuilder.setFilterNode(filterNode);
        } else {
            FilterNodes.Builder filterNodesBuilder = FilterNodes.newBuilder();
            for (int i = 0; i < keys.length; i++) {
                byte[] key = keys[i];
                filterNodesBuilder.addKeys(ByteString.copyFrom(key));
                filterNodesBuilder.addFilterNodes(filterNode);
            }
            pushDownBuilder.setFilterNodes(filterNodesBuilder.build());
        }
        return pushDownBuilder.build().toByteArray();
    }

    private FilterNode generateFilterNode(FilterType filterType) {
        FilterNode.Builder filterNodeBuilder = FilterNode.newBuilder().setFilterType(filterType);
        switch (filterType) {
            case EMPTY:
            case ONLY_VERTEX:
            case IN_EDGE:
            case OUT_EDGE:
            case VERTEX_VALUE_DROP:
            case EDGE_VALUE_DROP:
            case VERTEX_MUST_CONTAIN:
                break;
            case AND:
            case OR:
                filterNodeBuilder.addFilters(generateFilterNode(FilterType.VERTEX_TS))
                    .addFilters(generateFilterNode(FilterType.OUT_EDGE));
                break;
            case VERTEX_TS:
            case EDGE_TS:
                filterNodeBuilder.setLongContent(
                    LongList.newBuilder().addAllLong(Arrays.asList(0L, 10L)));
                break;
            case MULTI_EDGE_TS:
                FilterNode.Builder builder = FilterNode.newBuilder()
                    .setFilterType(FilterType.EDGE_TS)
                    .setLongContent(LongList.newBuilder().addAllLong(Arrays.asList(10L, 20L)));
                filterNodeBuilder.setFilterType(FilterType.OR)
                    .addFilters(generateFilterNode(FilterType.EDGE_TS))
                    .addFilters(builder.build());
                break;
            case VERTEX_LABEL:
            case EDGE_LABEL:
                filterNodeBuilder.setStrContent(StringList.newBuilder().addStr("foo"));
                break;
            case TTL:
            case GENERATED:
            case OTHER:
            default:
                throw new RuntimeException("unsupported filter type " + filterType.name());
        }
        return filterNodeBuilder.build();
    }

    @Test
    public void test() {
        NativeGraphStore store = buildStore();
        testPointGetWithoutPushDown(store);
        testMultiPointGetWithoutPushDown(store);
        testScanWithoutPushDown(store);

        for (int i = 100; i < 200; i++) {
            VertexContainer vertexContainer = new VertexContainer(("a" + i).getBytes(), i,
                "foo" + i, ("b" + i).getBytes());
            store.addVertex(vertexContainer);
            EdgeContainer edgeContainer = new EdgeContainer(("a" + i).getBytes(), i, "foo" + i,
                i % 2 == 0, "d".getBytes(), ("b" + i).getBytes());
            store.addEdge(edgeContainer);
        }
        store.flush();

        // pointGet, multiPointGet, scan vertex test.
        List<FilterType> vertexFilterList = Arrays.asList(FilterType.EMPTY,
            FilterType.ONLY_VERTEX, FilterType.OR, FilterType.VERTEX_VALUE_DROP, FilterType.AND,
            FilterType.VERTEX_TS, FilterType.VERTEX_LABEL);
        for (FilterType filterType : vertexFilterList) {
            testPointGetVertexWithPushDown(store, filterType);
            testMultiPointGetVertexWithPushDown(store, filterType);
            testScanVertexWithPushDown(store, filterType);
        }

        // pointGet, multiPointGet, scan edges test.
        List<FilterType> edgesFilterList = Arrays.asList(FilterType.EMPTY, FilterType.IN_EDGE,
            FilterType.OUT_EDGE, FilterType.EDGE_VALUE_DROP, FilterType.AND, FilterType.OR,
            FilterType.EDGE_TS, FilterType.MULTI_EDGE_TS, FilterType.EDGE_LABEL);
        for (FilterType filterType : edgesFilterList) {
            testPointGetEdgesWithPushDown(store, filterType);
            testMultiPointGetEdgesWithPushDown(store, filterType);
            testScanEdgesWithPushDown(store, filterType);
        }

        // pointGet, multiPointGet, scan vertexAndEdge test.
        List<FilterType> vertexAndEdgeFilterList = Arrays.asList(FilterType.EMPTY,
            FilterType.ONLY_VERTEX, FilterType.IN_EDGE, FilterType.OUT_EDGE,
            FilterType.VERTEX_VALUE_DROP, FilterType.EDGE_VALUE_DROP,
            FilterType.VERTEX_MUST_CONTAIN, FilterType.AND, FilterType.OR, FilterType.VERTEX_TS,
            FilterType.EDGE_TS, FilterType.MULTI_EDGE_TS, FilterType.VERTEX_LABEL,
            FilterType.EDGE_LABEL);
        for (FilterType filterType : vertexAndEdgeFilterList) {
            testPointGetVertexAndEdgeWithPushDown(store, filterType);
            testMultiPointGetVertexAndEdgeWithPushDown(store, filterType);
            testScanVertexAndEdgeWithPushDown(store, filterType);
        }

        testMultiPointGetVertexAndEdgeWithPushDown(store, FilterType.AND);

        testScanVertexWithPushDown(store, FilterType.VERTEX_VALUE_DROP);
        testScanEdgesWithPushDown(store, FilterType.EDGE_VALUE_DROP);
        testScanVertexAndEdgeWithPushDown(store, FilterType.VERTEX_VALUE_DROP);
        testScanVertexAndEdgeWithPushDown(store, FilterType.EDGE_VALUE_DROP);

        store.close();
    }

    @Test(enabled = false)
    public void testEdgeSpeed() {
        NativeGraphStore store = buildStore1();
        for (int i = 0; i < 10; i++) {
            int count = 0;
            long s = System.nanoTime();
            try (CStoreEdgeIterator it = store.scanEdges()) {
                while (it.hasNext()) {
                    count++;
                    it.next();
                }
            }
            System.out.println("getEdge " + (System.nanoTime() - s) / count);
        }
        store.close();
    }

    private NativeGraphStore buildStore1() {
        Map<String, String> config = new HashMap<>();
        config.put("hello", "world");
        config.put("foo", "bar");

        NativeGraphStore store = new NativeGraphStore("cstore", 1, config);

        for (int i = 0; i < 100000; i++) {
            VertexContainer vertexContainer = new VertexContainer(
                ("a" + i).getBytes(), i, "foo", ("b" + i).getBytes());
            store.addVertex(vertexContainer);
            for (int j = 0; j < 50; j++) {
                EdgeContainer edgeContainer = new EdgeContainer(
                    ("a" + i).getBytes(), i, "foo", i % 2 == 0, ("d" + j).getBytes(), ("b" + i).getBytes());
                store.addEdge(edgeContainer);
            }
        }
        store.flush();
        return store;
    }
}