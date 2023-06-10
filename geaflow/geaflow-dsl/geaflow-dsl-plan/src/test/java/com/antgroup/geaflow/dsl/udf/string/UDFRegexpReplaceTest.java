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

import static org.testng.Assert.assertEquals;

import com.antgroup.geaflow.dsl.udf.table.string.RegExpReplace;
import org.testng.annotations.Test;

public class UDFRegexpReplaceTest {

    @Test
    public void test() {
        RegExpReplace udf = new RegExpReplace();
        String res = udf.eval("100-200", "(\\d+)-(\\d+)", "1");
        assertEquals(res, "1");
        assertEquals(udf.eval("(adfafa", "\\(", ""), "adfafa");

        assertEquals(udf.eval("(adfafa", "\\(", ""), "adfafa");

        assertEquals(udf.eval("adf\"afa", "\"", ""), "adfafa");

        assertEquals(udf.eval("adf\"afa", "\"", ""), "adfafa");

        assertEquals(udf.eval("adfabadfasdf", "[a]", "3"), "3df3b3df3sdf");

    }
}
