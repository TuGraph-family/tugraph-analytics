# DML
## Insert Table

**Syntax**
```
INSERT INTO <table name> 
<table query>
;
```

**Example**
例子1
```sql
INSERT INTO table VALUES ('json', 111);
```
这个例子向**table**表导入一行数据。

例子2
```sql
INSERT INTO user_table
SELECT id, age FROM users
WHERE age > 20;
```
这个例子向**user_table**表导入一个查询语句结果。

例子3
```sql
INSERT INTO tbl_result
MATCH (a:person where a.id = 1) -[e:knows]->(b:person)
RETURN a.id as a_id, e.weight as weight, b.id as b_id;
```
这个例子向**tbl_result**表导入一个走图查询语句返回的结果。


## Insert Graph
Insert命令还可以向图导入数据。与表不同，图使用GeaFlow自主维护的存储。

## 插入点/边
Insert命令向图中的点或边导入数据时，操作对象以点分的图名加点边名表示，支持字段的重新排序。

**Syntax**
```
INSERT INTO <graph name>.<vertex/edge name>
[(<field name> [{, <field name>} ... ])]
<table query>
;
```

**Example**
例子1
```sql
INSERT INTO dy_modern.person
SELECT cast(id as bigint), name, cast(other as int) as age
FROM modern_vertex WHERE type = 'person'
;
```
这个例子向图**dy_modern**中的点**person**导入来自源表**modern_vertex**的数据，源表modern_vertex的三个字段与person中的字段一一对应。

例子2
```sql
INSERT INTO dy_modern.person(name, id, age)
SELECT 'jim', 1,  20
UNION ALL
SELECT 'kate', 2, 22
;
```
这个例子向图**dy_modern**中的点**person**导入两行数据，数据的字段以"name, id, age"顺序排列，对应点表person的同名字段。假设person还具有其他字段，则那些字段自动填充空值null。

例子3
```sql
INSERT INTO dy_modern.knows
SELECT 1, 2, 0.2
;
```
这个例子向图**dy_modern**中的边**knows**导入一行数据。

## 多表插入
有时源表需要同时插入到多个点或边中，特别是源表的外键表示一种关系时，往往需要转化为一类边，键值也将成为边的对端点。INSERT语句也支持这种单一源表，多目标点的插入。

**Syntax**
```
INSERT INTO <graph name>
(<vertex/edge name>.<field name> [{, <vertex/edge name>.<field name>} ... ])
<table query>
;
```
不同于向点边整体插入数据，这种写法将点边名字放到括号内，每个字段都需要指定所属的节点。和前文一样的是，未被指定的字段自动填充空值null。如果有meta字段(id或timestamp)未被指定，语法检查将报错。

**Example**
例子1
```sql
INSERT INTO dy_graph(
  Country.id, Country.name, Country.url, isPartOf.srcId, isPartOf.targetId
)
SELECT CAST(id as BIGINT), name, url,
CAST(id as BIGINT), CAST(PartOfPlaceId as BIGINT)
FROM tbl_vertex_place
Where type = 'Country';
```
这个例子向图**dy_graph**中的点**Country**和边**isPartOf**同时插入。源表中的**PartOfPlaceId**作为一个外键表示国家所属的洲，转化为出边isPartOf。

例子2
```sql
INSERT INTO dy_graph(
  Tag.id, Tag.name, Tag.url, hasType.srcId, hasType.targetId, TagClass.id
)
SELECT CAST(id as BIGINT), name, url,
CAST(id as BIGINT), CAST(TypeTagClassId as BIGINT),
CAST(TypeTagClassId as BIGINT)
FROM tbl_vertex_tag
Where length(url) > 3;
```
这个例子向图**dy_modern**中的点**Tag**,边**hasType**,点**TagClass**同时插入数据，源表中的**TypeTagClassId**转化为Tag的hasType类型边，同时插入新的TagClass点。假设TagClass还具有其他字段，则那些字段自动填充空值null。