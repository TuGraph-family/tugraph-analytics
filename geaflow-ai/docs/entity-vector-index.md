<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Entity vector index

`EntityVectorIndex` is the entity adapter for
[#853](https://github.com/apache/geaflow/issues/853). It implements `IndexStore`
and wraps an existing `EmbeddingIndexStore`. Index construction and legacy JSONL
loading remain in that store. The wrapper makes entity identity, embedding
provenance, and validation explicit.

## Bind known build metadata

```java
// legacyStore has already been initialized with EmbeddingIndexStore.initStore(...).
// These values must describe the embeddings already stored in that index.
EntityVectorMetadata metadata = new EntityVectorMetadata(
    "model-used-to-build-the-index", 768, "index-1", "vectors-1",
    Instant.parse("2026-01-01T00:00:00Z"));
EntityVectorIndex index = new EntityVectorIndex(legacyStore, metadata);

List<EntityVectorRecord> records = index.getEntityVectors(vertex);
```

The binding exposes `modelName`, `dimension`, `distance` (cosine), `indexVersion`,
`createdAt`, `formatVersion` (1), and `vectorVersion`. Model identity should include
the model revision when the provider can change a model behind a mutable alias.
The index version identifies the index generation; the vector version identifies
the embedding/segmentation generation. Change the vector version when rebuilding
with a different model or segmentation. Creation time is supplied by the build,
and is never used to generate entity or vector IDs.

Legacy JSONL contains only the entity key and embedding. It cannot prove which
model generated a vector. Binding is an explicit assertion by a caller that has
the original build provenance; the wrapper does not infer it from the query or
current service configuration. Preserve this provenance alongside the index in
the application's configuration/registry and supply it again after restart.
If provenance is unknown, rebuild before enabling validated search.

`new EntityVectorIndex(legacyStore)` provides legacy `getEntityIndex` and operator
compatibility without asserting provenance. Its typed lookup and `search` methods
reject missing metadata. It does not offer model validation for the legacy path.

## Query and result semantics

```java
VectorSearch query = new VectorSearch("memory", "session");
query.setEmbeddingModel("model-used-to-build-the-index");
query.addVector(new EmbeddingVector(queryEmbedding));

// null includes both vertices and edges; EntityType.VERTEX/EDGE restricts the kind.
List<EntityVectorHit> hits = index.search(graphAccessor, query, 10, 0.5, null);
```

Search validates the query model and dimensions and then validates all stored
candidate vectors before scoring, even for an empty index or a restricted entity
kind. Wrong dimensions, missing/different query models, nonfinite coordinates,
and ambiguous legacy entity keys are rejected. Empty embeddings lists represent
entities with no indexed text and produce no hits; zero vectors score zero.

Each entity's score is the maximum cosine across all of its segments and all
query vectors, starting at zero, with a strict `score > threshold` filter. Top-k
counts entities, not segments. A hit contains the winning segment, its entity
reference, metadata and score. Equal scores in this new API are ordered by entity
ID; equal winning segments retain their original segment order. This is linear
search with a full validation pass and in-memory snapshots, not an ANN index.
Callers must synchronize updates to the underlying store with reads/searches.

Entity identity follows the existing graph model: `(label, vertex ID)` for a
vertex and `(label, source ID, destination ID)` for a directed edge. IDs encode the
entity kind and length-prefixed components (Java UTF-16 string lengths), so field
boundaries and vertex/edge namespaces cannot collide. As in the current graph
model, edges with the same label and endpoints share one identity. Vector IDs add
the vector version and segment ordinal. Duplicate edge visits during graph scans
produce one entity hit.

The existing JSONL key format concatenates components without delimiters. The
wrapper's full validation pass rejects a graph whose distinct entities have the
same legacy key when directly wrapping `EmbeddingIndexStore`. Typed IDs cannot
repair information already lost in such a file; rebuild/migrate that index.

## Existing operator and server

Register the wrapper with `GraphMemoryServer.addIndexStore(index)` in place of
the underlying store. `GraphMemoryServer` dispatches it to `EmbeddingOperator`.
For a bound wrapper, the operator validates query provenance and stored vectors
before scoring. Set `VectorSearch.embeddingModel` when querying it. Empty query
lists retain the existing operator's no-op behavior.

The operator retains its existing score aggregation, threshold, top-N, tie
behavior, global vertex-only candidate scan, and subgraph expansion. Vertex/edge
lookup is exposed by the wrapper without adding edges to the operator's global
seed set. Registering both the wrapper and its delegate would execute retrieval
twice, so register only one.

## VectorStore integration

This first implementation builds against the current repository without adding a
second `VectorStore` SPI or copying the unmerged
[#851 / PR #855](https://github.com/apache/geaflow/pull/855) implementation.
`IndexStore` delegation and immutable entity records isolate the adaptation point.
Once that SPI is finalized, map entity IDs to source IDs, retain entity kind and
vector version as entity metadata, and reuse its model metadata and storage
validation. The entity-level maximum-score aggregation must occur before top-k;
truncating segment hits first would change retrieval results. Persistent metadata
and migration should use the shared VectorStore contract rather than introducing
another file format here. That integration is pending, not implemented by this PR.

Related embedding correctness work:
[#830](https://github.com/apache/geaflow/issues/830) /
[#832](https://github.com/apache/geaflow/pull/832). Existing text filtering and
embedding lifecycle tests remain applicable.

## Tests

`EntityVectorIndexTest` uses fixed vectors and local JSONL fixtures without a
remote model. It covers entity identity, multiple segments, defensive copies,
typed search and filters, validation failures, legacy key collisions, reload,
operator compatibility and server dispatch. Run it together with the existing
embedding regression tests:

```bash
mvn -pl geaflow-ai -am \
  -Dtest=EntityVectorIndexTest,EmbeddingIndexLifecycleTest,IgnorableTextFilterTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

The repository's JaCoCo 0.8.8 cannot instrument Java 21 runtime classes. For a
local Java 21 test run, append `-Djacoco.skip=true -DcoverageAgent=` to disable
coverage collection, or use a JDK supported by that JaCoCo version.
