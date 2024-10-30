# DDL
# 表相关DDL
## Create Table
该命令用来创建一张表，GeaFlow将其识别为外部表并将元数据存储在Catalog中。

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
	name string,
	age int
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/persons.txt'
);
```
这个例子创建了一张表**v_person_table**，包含id, name, age三列，表的存储类型为文件，并通过**geaflow.dsl.file.path**参数说明需要访问的文件存放在引擎资源的指定目录中。

### 数据类型

| 类型 | 说明 |
| -------- | -------- |
| BOOLEAN     | 布尔类型    |
| SHORT     | 短整形类型，范围: -2^15 + 1 ~ 2^15-1     |
| INT     |整形类型， 范围: -2^31 + 1 ~ 2^31-1     |
| LONG     | 长整形类型，范围: -2^63 + 1 ~ 2^63-1     |
| DOUBLE     |双精度浮点类型，范围: -2^1024 ~ +2^1024     |
| VARCHAR     |字符串类型  |
| TIMESTAMP   | 时间戳类型 |

### 参数
创建表的同时，可以使用WITH指定表的参数信息，其中type参数用于指定外部表的存储类型，其他参数为kv类型。

**Example**
```sql
CREATE TABLE person (
	id   VARCHAR,
	name VARCHAR,
	age INT
) WITH (
	type ='file',
	geaflow.dsl.file.path = '/path/to/person/',
	geaflow.dsl.column.separator = '|',
	geaflow.dsl.window.size = 5000
);
```
这个例子创建一个文件类型的表，并指定表参数。其中type指定表类型为文件; geaflow.dsl.file.path文件路径;geaflow.dsl.column.separator指定字段分隔列; geaflow.dsl.window.size指定每批次读取文件的行数。


## Create View

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

# 图相关DDL
## Create Graph

**Syntax**
一个图至少包含一对点边，点表必须包含一个id字段作为主键，边表必须包含srcId和targetId作为主键，边表还可以有一个时间戳字段标识时间。

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
这个例子创建了一张包含2个点2个边的图，存储类型为rocksdb, 分片数2个。

### 图存储
图的存储类型可以在WITH关联的配置列表中使用storeType配置项指定，目前GeaFlow支持Memory, RocksDB作为图存储格式。

图的存储分片数通过shardCount配置项指定，图的存储分片数影响图计算时的并发数，设为更大的值可以利用更多机器并发计算，但所需资源数也将增长。

# 自定义函数
## Create Function
这个命令用来引入一个自定义函数。

**Syntax**
```
CREATE FUNCTION <function name> AS <implementation class>
```

**Example**
```sql
CREATE FUNCTION mysssp AS 'com.antgroup.geaflow.dsl.udf.graph.SingleSourceShortestPath';
```