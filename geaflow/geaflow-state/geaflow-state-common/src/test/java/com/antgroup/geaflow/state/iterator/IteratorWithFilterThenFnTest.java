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

package com.antgroup.geaflow.state.iterator;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import org.testng.Assert;
import org.testng.annotations.Test;

public class IteratorWithFilterThenFnTest {

    @Test
    public void testFunction() {
        IteratorWithFilterThenFn<Integer, String> it =
            new IteratorWithFilterThenFn<>(Arrays.asList(1, 2, 3, 4, 5).iterator(),
            o -> o > 3, Object::toString);

        ArrayList<String> list = Lists.newArrayList(it);
        Assert.assertEquals(list.size(), 2);
        Assert.assertTrue(list.contains("4"));
        Assert.assertTrue(list.contains("5"));

        it = new IteratorWithFilterThenFn<>(Arrays.asList(1, 2, 3, 4, 5).iterator(),
            o -> o <= 3, Objects::toString);
        list = Lists.newArrayList(it);
        Assert.assertEquals(list.size(), 3);
    }
}