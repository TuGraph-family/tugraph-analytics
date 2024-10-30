# Use Graph
You must set current using graph before execute graph match query.
## Syntax

```sql
USE GRAPH Identifier
```
## Example

```sql
-- Set current using graph.
USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	a.id,
	b.id,
	c.id,
	c.kind,
	d.id,
	d.type
FROM (
  MATCH (a) -> (b) where b.id > 0 and a.lang is null
  MATCH (a) <- (c) where label(c) = 'person'
  Let c.kind = 'k' || cast(c.age / 10 as varchar)
  MATCH (c) -> (d) where d != b
  Let d.type = if (label(d) = 'person', 1, 0)
  RETURN a, b, c, d
)
;
```
# Use Instance
An Instance is similar to the database in Hive/Mysql. We can specify the instance for
the table/function/graph queryed by the follow dsl.
## Syntax

```sql
USE INSTANCE Identifier
```
## Example

```sql
Use instance geaflow;
USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	a.id,
	b.id,
	c.id,
	c.kind,
	d.id,
	d.type
FROM (
  MATCH (a) -> (b) where b.id > 0 and a.lang is null
  MATCH (a) <- (c) where label(c) = 'person'
  Let c.kind = 'k' || cast(c.age / 10 as varchar)
  MATCH (c) -> (d) where d != b
  Let d.type = if (label(d) = 'person', 1, 0)
  RETURN a, b, c, d
);
```