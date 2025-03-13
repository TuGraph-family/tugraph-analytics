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

package com.antgroup.geaflow.com.antgroup.geaflow.api.window;

import com.antgroup.geaflow.api.window.IWindow;
import com.antgroup.geaflow.api.window.WindowFactory;
import com.antgroup.geaflow.api.window.impl.AllWindow;
import com.antgroup.geaflow.api.window.impl.SizeTumblingWindow;
import org.testng.Assert;
import org.testng.annotations.Test;

public class WindowFactoryTest {

    @Test
    public void testCreateSizeTumblingWindow() {
        IWindow window = WindowFactory.createSizeTumblingWindow(10);
        Assert.assertTrue(window instanceof SizeTumblingWindow);
    }

    @Test
    public void testAllWindow() {
        IWindow window = WindowFactory.allWindow();
        Assert.assertTrue(window instanceof AllWindow);
    }
}
