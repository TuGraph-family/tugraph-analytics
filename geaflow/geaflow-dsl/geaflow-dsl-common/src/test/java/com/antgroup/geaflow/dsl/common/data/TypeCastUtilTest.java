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

package com.antgroup.geaflow.dsl.common.data;

import com.antgroup.geaflow.common.binary.BinaryString;
import com.antgroup.geaflow.dsl.common.util.TypeCastUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TypeCastUtilTest {

    @Test
    public void testIntCast() {
        Integer i = 1;
        Assert.assertEquals(TypeCastUtil.cast(i, Integer.class), 1);
        Assert.assertEquals(TypeCastUtil.cast(i, Long.class), 1L);
        Assert.assertEquals(TypeCastUtil.cast(i, Double.class), 1.0);
        Assert.assertEquals(TypeCastUtil.cast(i, BinaryString.class), BinaryString.fromString("1"));
    }

    @Test
    public void testCastDouble() {
        Double d = 1.0;
        Assert.assertEquals(TypeCastUtil.cast(d, Integer.class), 1);
        Assert.assertEquals(TypeCastUtil.cast(d, Long.class), 1L);
        Assert.assertEquals(TypeCastUtil.cast(d, Double.class), 1.0);
        Assert.assertEquals(TypeCastUtil.cast(d, BinaryString.class), BinaryString.fromString("1.0"));
    }
}
