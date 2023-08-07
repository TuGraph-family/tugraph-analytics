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

package com.antgroup.geaflow.dsl.runtime.data;

import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.data.impl.types.DoubleVertex;
import com.antgroup.geaflow.dsl.common.data.impl.types.IntVertex;
import com.antgroup.geaflow.dsl.common.data.impl.types.LongVertex;
import com.antgroup.geaflow.dsl.runtime.traversal.data.FieldAlignDoubleVertex;
import com.antgroup.geaflow.dsl.runtime.traversal.data.FieldAlignIntVertex;
import com.antgroup.geaflow.dsl.runtime.traversal.data.FieldAlignLongVertex;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RuntimeVertexTest {

    @Test
    public void testLongVertex() {
        LongVertex test = new LongVertex(1);
        test.withLabel("v");
        test.withValue(ObjectRow.EMPTY);
        LongVertex test2 = new LongVertex(1);
        test2.withLabel("v");
        test2.withValue(ObjectRow.EMPTY);
        Assert.assertEquals(test.hashCode(), test2.hashCode());
        Assert.assertEquals(test, test2);

        FieldAlignLongVertex test3 = new FieldAlignLongVertex(test2, new int[]{0, 1});
        test3.withLabel("v");
        test3.withValue(ObjectRow.EMPTY);
        Assert.assertEquals(test.hashCode(), test3.hashCode());
        Assert.assertEquals(test, test3);
        FieldAlignLongVertex test4 = new FieldAlignLongVertex(test, new int[]{0, 1});
        Assert.assertEquals(test3, test4);
    }

    @Test
    public void testIntVertex() {
        IntVertex test = new IntVertex(1);
        test.withLabel("v");
        test.withValue(ObjectRow.EMPTY);
        IntVertex test2 = new IntVertex(1);
        test2.withLabel("v");
        test2.withValue(ObjectRow.EMPTY);
        Assert.assertEquals(test.hashCode(), test2.hashCode());
        Assert.assertEquals(test, test2);

        FieldAlignIntVertex test3 = new FieldAlignIntVertex(test2, new int[]{0, 1});
        test3.withLabel("v");
        test3.withValue(ObjectRow.EMPTY);
        Assert.assertEquals(test.hashCode(), test3.hashCode());
        Assert.assertEquals(test, test3);
        FieldAlignIntVertex test4 = new FieldAlignIntVertex(test, new int[]{0, 1});
        Assert.assertEquals(test3, test4);
    }

    @Test
    public void testDoubleVertex() {
        DoubleVertex test = new DoubleVertex(1);
        test.withLabel("v");
        test.withValue(ObjectRow.EMPTY);
        DoubleVertex test2 = new DoubleVertex(1);
        test2.withLabel("v");
        test2.withValue(ObjectRow.EMPTY);
        Assert.assertEquals(test.hashCode(), test2.hashCode());
        Assert.assertEquals(test, test2);

        FieldAlignDoubleVertex test3 = new FieldAlignDoubleVertex(test2, new int[]{0, 1});
        test3.withLabel("v");
        test3.withValue(ObjectRow.EMPTY);
        Assert.assertEquals(test.hashCode(), test3.hashCode());
        Assert.assertEquals(test, test3);
        FieldAlignDoubleVertex test4 = new FieldAlignDoubleVertex(test, new int[]{0, 1});
        Assert.assertEquals(test3, test4);
    }
}

