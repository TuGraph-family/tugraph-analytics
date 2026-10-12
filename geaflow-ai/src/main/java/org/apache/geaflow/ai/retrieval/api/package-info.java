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

/**
 * Versioned retrieval API contract for Java and HTTP clients.
 *
 * <p>Version {@code v1} supports {@code KEYWORD} retrieval with {@code SEQUENTIAL} execution.
 * {@code PARALLEL}, {@code CASCADED}, and non-empty query vectors are reserved and must be
 * reported as {@code UNSUPPORTED_OPTION}. Unknown JSON fields are accepted for additive wire
 * compatibility; duplicate fields are rejected. Response collections are always JSON arrays.</p>
 *
 * <p>Error codes map to HTTP status as follows: {@code INVALID_REQUEST}=400,
 * {@code UNSUPPORTED_OPTION}=400, {@code GRAPH_NOT_FOUND}=404,
 * {@code INDEX_NOT_READY}=503, {@code RETRIEVAL_TIMEOUT}=504, and
 * {@code INTERNAL_ERROR}=500. Only {@code INDEX_NOT_READY} and {@code RETRIEVAL_TIMEOUT} are
 * retriable.</p>
 */
package org.apache.geaflow.ai.retrieval.api;
