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

package com.antgroup.geaflow.dsl.udf.string;

import com.antgroup.geaflow.dsl.udf.table.string.Substr;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SubstrTest {

    @Test
    public void test() {
        Substr sb = new Substr();

        Assert.assertEquals(sb.eval("01021000100000000017", 1, 1), "0");
        Assert.assertEquals(sb.eval("01021000100000000017", 2, 1), "1");
        Assert.assertNull(sb.eval("01021000100000000017", null, 1));
        Assert.assertEquals(sb.eval("Facebook", 5), "book");
        Assert.assertEquals(sb.eval("Facebook", -5), "ebook");
        Assert.assertEquals(sb.eval("Facebook", 5, 1), "b");
    }
}
