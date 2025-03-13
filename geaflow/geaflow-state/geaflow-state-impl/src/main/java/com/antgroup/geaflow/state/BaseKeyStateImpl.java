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

package com.antgroup.geaflow.state;

import com.antgroup.geaflow.state.context.StateContext;
import com.antgroup.geaflow.state.manage.ManageableState;
import com.antgroup.geaflow.state.manage.ManageableStateImpl;
import com.antgroup.geaflow.state.strategy.manager.IKeyStateManager;
import com.antgroup.geaflow.state.strategy.manager.KeyStateManager;

public abstract class BaseKeyStateImpl<K> {

    protected final IKeyStateManager<K> keyStateManager;
    protected final ManageableState manageableState;

    public BaseKeyStateImpl(StateContext context) {
        this.keyStateManager = new KeyStateManager<>();
        this.keyStateManager.init(context);
        this.manageableState = new ManageableStateImpl(this.keyStateManager);
    }

    public ManageableState manage() {
        return manageableState;
    }
}
