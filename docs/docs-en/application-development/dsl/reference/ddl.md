# DDL
# Table
## Create Table
This command is used to create a table in GeaFlow, which belongs to external tables and is stored in catalog.

**Syntax**

```
CREATE TABLE <table name> 
(
	<column name> <data type>
	[ { , <column name> <data type> } ... ]
) WITH （
	type = <table type>
	[ { , <config key> = <config value> } ... ]
);
```
**Example**
```sql
Create Table v_person_table (
	id bigint,
	name varchar,
	age int
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/persons.txt'
);
```
This example creates a table called **v_person_table**, which includes three columns: id, name, age. The storage type of the table is a file (or files), and the **geaflow.dsl.file.path** config specifies that the file to be accessed is located in a specified directory in the engine's resources.



### Data Type

| Type Name | Value | 
| -------- | -------- | 
| BOOLEAN     | true, false     | 
| SHORT     | Range: -2^15 + 1 ~ 2^15-1     | 
| INT       | Range: -2^31 + 1 ~ 2^31-1     | 
| BIGINT     | Range: -2^63 + 1 ~ 2^63-1     |
| DOUBLE     | Range: -2^1024 ~ +2^1024     | 
| VARCHAR     | Variable length character string     | 

### External Tables
When creating a table, the **WITH** keyword can be used to associate the table with its configs, which only apply to that table. By specifying configs, GeaFlow's table can access external tables. The **type** config is used to specify the storage type of the external table.

**Example**
```sql
CREATE TABLE tbl_vertex_person (
	id VARCHAR,
	firstName VARCHAR,
	lastName VARCHAR,
	gender VARCHAR
) WITH (
	type ='file',
	geaflow.dsl.file.path = '/social_network/dynamic',
	geaflow.dsl.column.separator = '|',
	geaflow.dsl.window.size = 5000,
	geaflow.dsl.file.name.regex = '^[(?i)p]erson[_][0-9].*'
);
```
This example creates a table of file type and specifies some associated configs.
From top to bottom, "**type**" specifies the storage type is file.
"**geaflow.dsl.file.path**" specifies that the stored files are located in a folder.
"**geaflow.dsl.column.separator**" specifies that the content in the file is separated by vertical bars.
"**geaflow.dsl.window.size**" specifies the number of lines to read from the file in each batch.
"**geaflow.dsl.file.name.regex**" specifies the regular expression pattern used to filter file names. The table only needs to read files in the folder containing the word "person" in their names.

For details of the external table types and their corresponding usage supported by GeaFlow, please refer to the Connector section.

## Create View
This command is used to create a temporary table view that represents the query result.

**Syntax**
```
CREATE VIEW <table veiw name> 
(
	<column name>
	[ { , <column name>} ... ]
) AS
	<table query>
;
```

**Example**
```sql
CREATE VIEW console_1 (a, b, c) AS
SELECT id, name, age FROM v_person_table;
```

# Graph
## Create Graph
This command is used to create a graph. For graphs, GeaFlow has self-maintained storage.

**Syntax**
A graph must contain at least one pair of vertex and edge. The vertex table must include an "**ID**" field as identifier, and the edge table must include a pair of "**source id**" and "**destination id**" fields as identifiers. The edge table may also have a "**timestamp**" field to represent time.

```
CREATE GRAPH <graph name> 
(
	<graph vertex>
	[ { , <graph vertex> } ... ]
	, <graph edge>
	[ { , <graph edge> } ... ]
) WITH （
	storeType = <graph store type>
	[ { , <config key> = <config value> } ... ]
);

<graph vertex>  ::=
VERTEX <vertex name>
(
	<column name> <data type> ID
	[ {, <column name> <data type> } ... ]
)

<graph edge>  ::=
Edge <edge name>
(
	<column name> <data type> SOURCE ID
	, <column name> <data type> DESTINATION ID
	[ , <column name> <data type> TIMESTAMP ]
	[ {, <column name> <data type> } ... ]
)

```

**Example**
```sql
CREATE GRAPH dy_modern (
	Vertex person (
		id bigint ID,
		name varchar,
		age int
	),
	Vertex software (
		id bigint ID,
		name varchar,
		lang varchar
	),
	Edge knows (
		srcId bigint SOURCE ID,
		targetId bigint DESTINATION ID,
		weight double
	),
	Edge created (
		srcId bigint SOURCE ID,
		targetId bigint DESTINATION ID,
		weight double
	)
) WITH (
	storeType = 'rocksdb',
	shardCount = 2
);
```
This example creates a graph "**dy_modern**" that is divided into two shards and stored in RocksDB. The graph has two types of nodes and edges. Nodes "**Person**" and "**Software**" both have an "**id**" field of type long as the identifier, and they also have a "name" field. Nodes of type "Person" have an additional field "age", and nodes of type "Software" have a field "lang". Edges of type "**knows**" and "**created**" have "**srcId**" and "**targetId**" fields of type long as source and destination identifiers respectively. They do not have a timestamp, but both have a "weight" field.


### Vertex/Edge Type Rules
In theory, the vertex and edge fields can be named arbitrarily and assigned any type. However, an unreasonable graph schema can cause difficult problems for subsequent type calculations.

For example, "**Match (p)**" matches any type of vertex p, and for the dy_modern graph, which only has two types of vertex, it is equivalent to "**Match (p:person|software)**".Here, the **union calculation** of two types of nodes generates the "**person|software**" type, which is very common in graph traversal.

Vertex and edge types union follows the following rules:
* Vertex and edge types cannot be unionized with each other.
* The fields id/source id/destination id/timestamp belong to the meta fields and must have the same name and type in the union types.
* attribute fields with the same name must have the same type.

In summary, it is recommended to follow the following rules when creating the vertex/edge type of a graph:

* Use "id" as the identifier field name for vertex, and "srcId" and "targetId" as the identifier field names for edges;
* Use the same type for the "id" field of all vetex, and the same for edges;
* Use more open types for fields whenever possible to accommodate different data types, such as String, Long, Double, etc.
### Graph Store
The storage type of a graph can be specified in the configuration list associated with the **WITH** keyword using the "**storeType**" config. Currently, GeaFlow supports **Memory**, **RocksDB** as graph storage.

The number of storage shards for a graph can be specified using the "**shardCount**" config. The number of storage shards affects the parallelism of graph traversal. Setting a larger value can utilize more machines for parrallel computation, but will also require more resources.

# Function
## Create Function

This command is used to import a user defined function.

**Syntax**
```
CREATE FUNCTION <function name> AS <implementation class>
[ USING  <remote resource> ]
;
```

**Example**
```sql
CREATE FUNCTION mysssp AS 'com.antgroup.geaflow.dsl.udf.graph.SingleSourceShortestPath';
```

The implementation class of the UDF needs to inherit the **UserDefinedFunction** class or its subclass, please refer to the **User Defined Function** section for details. 
