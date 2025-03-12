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

import { useRequest } from "ahooks";
import {
  createEdge,
  createNode,
  deleteEdge,
  deleteNode,
  editEdge,
  editNode,
} from "../../../../services/GraphDataController";

export const useGraphData = () => {
  const {
    runAsync: onCreateEdge,
    loading: CreateEdgeLoading,
    error: CreateEdgeError,
  } = useRequest(createEdge, { manual: true });
  const {
    runAsync: onCreateNode,
    loading: CreateNodeLoading,
    error: CreateNodeError,
  } = useRequest(createNode, { manual: true });
  const {
    runAsync: onDeleteEdge,
    loading: DeleteEdgeLoading,
    error: DeleteEdgeError,
  } = useRequest(deleteEdge, { manual: true });
  const {
    runAsync: onDeleteNode,
    loading: DeleteNodeLoading,
    error: DeleteNodeError,
  } = useRequest(deleteNode, { manual: true });
  const {
    runAsync: onEditEdge,
    loading: EditEdgeLoading,
    error: EditEdgeError,
  } = useRequest(editEdge, { manual: true });
  const {
    runAsync: onEditNode,
    loading: EditNodeLoading,
    error: EditNodeeError,
  } = useRequest(editNode, { manual: true });

  return {
    onCreateEdge,
    CreateEdgeLoading,
    CreateEdgeError,
    onCreateNode,
    CreateNodeLoading,
    CreateNodeError,
    onDeleteEdge,
    DeleteEdgeLoading,
    DeleteEdgeError,
    onDeleteNode,
    DeleteNodeLoading,
    DeleteNodeError,
    onEditEdge,
    EditEdgeLoading,
    EditEdgeError,
    onEditNode,
    EditNodeLoading,
    EditNodeeError,
  };
};
