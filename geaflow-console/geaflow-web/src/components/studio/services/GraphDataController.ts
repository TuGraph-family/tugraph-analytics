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

import request from "./request";

/* Create Node*/
export async function createNode(params: CreateNode) {
  return request(`/api/data/node`, {
    method: "POST",
    data: {
      ...params,
    },
  });
}

/* Create Edge*/
export async function createEdge(params: CreateEdge) {
  return request(`/api/data/edge`, {
    method: "POST",
    data: {
      ...params,
    },
  });
}

/* Edit Node*/
export async function editNode(params: EditNode) {
  return request(`/api/data/node`, {
    method: "PUT",
    data: {
      ...params,
    },
  });
}

/* Edit Edge*/
export async function editEdge(params: EditEdge) {
  return request(`/api/data/edge`, {
    method: "PUT",
    data: {
      ...params,
    },
  });
}

/* Delete Node*/
export async function deleteNode(params: DeleteNode) {
  return request(`/api/data/node`, {
    method: "DELETE",
    data: {
      ...params,
    },
  });
}

/* Delete Edge*/
export async function deleteEdge(params: DeleteEdge) {
  return request(`/api/data/edge`, {
    method: "DELETE",
    data: {
      ...params,
    },
  });
}
