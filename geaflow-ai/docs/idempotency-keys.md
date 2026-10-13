# Extracted-record idempotency keys

`IdempotencyKeys` in `org.apache.geaflow.ai.common.id` defines stable IDs for
records produced by document ingestion. Its format is
`gf-id:v1:<kind>:<lowercase SHA-256 hex>`. The `v1` marker versions the binary
encoding; `schemaVersion` is a separate input that versions the extraction
schema. Changing either one changes the key.

| Kind | Inputs |
| --- | --- |
| `vertex` | Schema version, entity label, stable entity ID, fact properties |
| `edge` | Schema version, relation label, directed source and destination IDs, fact properties |
| `triple` | Schema version, typed subject, predicate, typed object |
| `chunk` | Schema version, stable document ID, source span, exact chunk content |
| `evidence` | Schema version, fact key, stable document ID, source span |

Fact keys identify semantic content across documents. The evidence key identifies
one occurrence of a fact in a source. A graph vertex's logical ID remains
separate from its fact key: a property change creates a new fact key without
requiring a second vertex. Chunks and evidence use half-open spans with
`0 <= start < end`. Callers must use a consistent offset unit and a stable
document ID across retries. The CLI uses the chunk ordinal as its span unit
for file imports; direct input uses its UTF-16 character range.

The encoder writes length-prefixed UTF-8 strings and type markers. Map entries
are sorted by their string keys, including nested maps. Lists retain order.
Strings are encoded exactly as provided; no case, whitespace, or Unicode
normalization is applied. Supported property values are `null`, `String`,
`Boolean`, standard boxed numeric types, `BigInteger`, `BigDecimal`, maps with
string keys, and lists of supported values. Numeric Java types remain distinct,
and `BigDecimal` retains scale. Unsupported values fail fast. The key contains
neither a clock value nor a Java object identity or insertion-order-dependent
serialization.

The current CLI uses the chunk key as its graph vertex ID. Its duplicate
insert therefore reuses the same ID. `EdgeGroup` also rejects a repeated edge
with the same label, direction, endpoints, and positional values. Because the
legacy graph schema has no version field, it uses `legacy-graph-v1` as the
schema-version input. Removing an edge retains the existing graph behavior of
matching by label and endpoints; its removed keys are released for reinsertion.
The server skips consolidation if the request inserted no entities. Existing
vertices with old time-based IDs are not migrated.
Future extracted-fact, triple, and evidence writers must persist or look up
these keys at their write boundary to make their storage idempotent;
calculating a key alone does not prevent duplicate records.
