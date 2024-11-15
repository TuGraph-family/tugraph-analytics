# GeaFlow Store API

GeaFlow Store API provides a unified storage interface for managing state data in graph computing.
This document introduces how to use and implement these storage interfaces.

## Core Interfaces

GeaFlow Store API includes the following core interfaces:

### IStoreBuilder

Interface for building storage instances. Main methods:

- `getStoreDesc()`: Get storage description
- `build()`: Build storage instance

### IBaseStore

The base interface for all storage implementations, defining basic storage operations:

- `init()`: Initialize storage
- `flush()`: Flush data to storage
- `close()`: Close storage connection

### IStatefulStore

State management storage interface, inheriting from IBaseStore, providing stateful management
operations:

- `archive()`: Archive current state
- `recovery()`: Recover state from specified position
- `recoveryLatest()`: Recover state from specified position
- `compact()`: Compress/merge historical states
- `drop()`: Drop all data

### IGraphStore

Graph data storage interface, inheriting from IStatefulStore, used for storing graph vertices and
edges:

- `addVertex()`: Add vertex
- `getVertex()`: Get vertex
- `addEdge()`: Add edge
- `getEdges()`: Get edges
- `getOneDegreeGraph()`: Get one degree Graph.

## Usage Examples

### Basic Graph Storage Example

```java
// 1. Create StoreBuilder
IStoreBuilder builder = new MemoryStoreBuilder();

// 2. Build storage instance
IGraphStore graphStore = (IGraphStore) builder.build();

// 3. Initialize storage
graphStore.init(context);

// 4. Use storage
// Add vertex
graphStore.addVertex(vertex);

// Get vertex
IVertex vertex = graphStore.getVertex(vertexId);

// Add edge
graphStore.addEdge(edge);

// Get edges
List<IEdge> edges = graphStore.getEdges(vertexId);

// 5. Close storage
graphStore.close();
```

### State Management Example

```java
// 1. Create stateful storage instance
IStatefulStore statefulStore = new MemoryStatefulStore();

// 2. Initialize
statefulStore.init(context);

// 3. State management operations
// Archive current state
statefulStore.archive();

// Recover from specific version
statefulStore.recovery(version);

// Compact historical states
statefulStore.compact(version);

// 4. Close storage
statefulStore.close();
```

## Implementing Custom Storage

To implement custom storage, you need to:

1. Implement `IStoreBuilder` interface to create storage builder
2. Implement corresponding storage interfaces based on requirements:
    - Basic storage: implement `IBaseStore`
    - State management storage: implement `IStatefulStore`
    - Graph data storage: implement `IGraphStore`
3. Configure SPI service in `resources/META-INF/services`

For examples, refer to implementations in `geaflow-store-memory` module:

- `MemoryStoreBuilder`
- `StaticGraphMemoryStore`

## Configuration

Specify storage implementation and related parameters through configuration:

```java
Configuration conf = new Configuration();
// Specify storage type
conf.put(StoreConfig.STORE_TYPE, "memory");

// State management configurations
conf.put(StoreConfig.STATE_RETENTION_TIME, "24h");  // State retention time
conf.put(StoreConfig.COMPACT_INTERVAL, "6h");       // State compaction interval

// Other storage related configurations
