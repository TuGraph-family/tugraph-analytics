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

package com.antgroup.geaflow.console.core.model.version;

import com.antgroup.geaflow.console.core.model.GeaflowName;
import com.antgroup.geaflow.console.core.model.file.GeaflowRemoteFile;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class GeaflowVersion extends GeaflowName {

    private GeaflowRemoteFile engineJarPackage;

    private GeaflowRemoteFile langJarPackage;

    private boolean publish;

    public static boolean md5Equals(GeaflowVersion left, GeaflowVersion right) {
        if (left == null && right == null) {
            return true;
        }

        if (left != null && right != null) {
            return GeaflowRemoteFile.md5Equals(left.engineJarPackage, right.engineJarPackage)
                && GeaflowRemoteFile.md5Equals(left.langJarPackage, right.langJarPackage);
        }

        return false;
    }

    @Override
    public void validate() {
        super.validate();
        Preconditions.checkNotNull(engineJarPackage, "Invalid engineJarPackage");
    }

}
