/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.utils.keygroup;

import org.testng.Assert;
import org.testng.annotations.Test;

public class KeyGroupTest {

    @Test
    public void testKeyGroupContains() {
        KeyGroup a = new KeyGroup(0, 100);
        KeyGroup b = new KeyGroup(0, 1);
        Assert.assertTrue(a.contains(b));

        KeyGroup c = new KeyGroup(0, 100);
        Assert.assertTrue(a.contains(c));

        KeyGroup d = new KeyGroup(9, 101);
        Assert.assertFalse(a.contains(d));
    }

}