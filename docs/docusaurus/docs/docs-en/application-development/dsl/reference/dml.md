# DML
# Insert Table
**Syntax**
```
INSERT INTO <table name> 
<table query>
;
```

**Example**
Example 1:
```sql
INSERT INTO test_table VALUES ('json', 111);
```
This example inserts a row of data into the **test_table** table.

Example 2:
```sql
INSERT INTO user_table
SELECT id, age FROM users
WHERE age > 20;
```
This example inserts a query result into the **user_table** table.

Example 3:
```sql
INSERT INTO tbl_result
MATCH (a:person where a.id = 1) -[e:knows]->(b:person)
RETURN a.id as a_id, e.weight as weight, b.id as b_id;
```
This example inserts a result returned by a graph traversal query into the **tbl_result** table.


# Insert Graph
Insert command can also insert data into the graph. Unlike tables, graphs use storage self-maintained by GeaFlow.

# Insert vertex/edge
When insert data into vertex or edge in the graph using the _INSERT_ command, the target node is represented by the graph name and vertex/edge name separated by a dot, and supports reordering of fields.

**Syntax**
```
INSERT INTO <graph name>.<vertex/edge name>
[(<field name> [{, <field name>} ... ])]
<table query>
;
```

**Example**
Example 1
```sql
INSERT INTO dy_modern.person
SELECT cast(id as bigint), name, cast(other as int) as age
FROM modern_vertex WHERE type = 'person'
;
```
This example inserts data from the source table **modern_vertex** into the vertex **person** in the graph **dy_modern**. The three fields in source table modern_vertex correspond to the fields in vertex person one-to-one.

Example 2
```sql
INSERT INTO dy_modern.person(name, id, age)
SELECT 'jim', 1,  20
UNION ALL
SELECT 'kate', 2, 22
;
```

This example inserts two rows of data into the vertex **person** in the graph **dy_modern**. The fields in the data are arranged in the order of "name, id, age", corresponding to the same fields in the vertex table person. Assuming that person has other fields, those fields are automatically **filled with null** values.

Example 3
```sql
INSERT INTO dy_modern.knows
SELECT 1, 2, 0.2
;
```
This example inserts one row into the edge **knows** in the graph **dy_modern**.

# Multi table insert
Sometimes the source table needs to be inserted into multiple nodes simultaneously, especially when the foreign key of the source table represents a relationship, which often needs to be transformed into a type of edge, and the foreign key value will also become the opposite endpoint of the edge. The INSERT statement also supports this type of insertion where a single source table has multiple target nodes.

**Syntax**
```
INSERT INTO <graph name>
(<vertex/edge name>.<field name> [{, <vertex/edge name>.<field name>} ... ])
<table query>
;
```
Unlike inserting data into nodes as a whole, this syntax puts the names of nodes in parentheses, and each field needs to specify the node to which it belongs. Similarly to the previous examples, any fields that are not specified will be automatically filled with null values. If any meta fields (id or timestamp) are not specified, a syntax check error will occur.

**Example**
Example 1
```sql
INSERT INTO dy_graph(
  Country.id, Country.name, Country.url, isPartOf.srcId, isPartOf.targetId
)
SELECT CAST(id as BIGINT), name, url,
CAST(id as BIGINT), CAST(PartOfPlaceId as BIGINT)
FROM tbl_vertex_place
Where type = 'Country';
```
This example inserts data into the node "**Country**" and the edge "**isPartOf**" in the graph "**dy_graph**" simultaneously. The foreign key "**PartOfPlaceId**" in the source table represents the continent to which the country belongs, and is transformed into an outgoing edge "isPartOf".

Example 2
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
This example inserts data into the nodes "**Tag**" and "**TagClass**", and the edge "**hasType**" in the graph "**dy_modern**" simultaneously. The foreign key "**TypeTagClassId**" in the source table is transformed into the edge type of "hasType", and a new node "TagClass" is also inserted. Assuming that "TagClass" has other fields, those fields will be automatically filled with null values.

Even with this syntax, each record in the source table can only trigger one insertion in one type of node or edge. If multiple insertions are required, multiple INSERT statements should be written.
