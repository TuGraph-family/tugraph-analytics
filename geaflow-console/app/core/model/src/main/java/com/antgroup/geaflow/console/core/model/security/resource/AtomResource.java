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

package com.antgroup.geaflow.console.core.model.security.resource;

import com.antgroup.geaflow.console.common.util.type.GeaflowResourceType;
import com.google.common.base.Preconditions;
import lombok.Getter;

public abstract class AtomResource extends GeaflowResource {

    @Getter
    protected final String id;

    public AtomResource(String id, GeaflowResourceType type, GeaflowResource parent) {
        super(type, parent);
        this.id = Preconditions.checkNotNull(id, "Invalid resource id");
    }

    @Override
    public boolean include(GeaflowResource other) {
        if (!(other instanceof AtomResource)) {
            return false;
        }

        AtomResource otherAtom = (AtomResource) other;
        if (this.id.equals(otherAtom.id) && this.type.equals(otherAtom.type)) {
            return true;
        }

        return include(other.parent);
    }
}
