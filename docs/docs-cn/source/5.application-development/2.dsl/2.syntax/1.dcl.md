# DCL

## Use Graph
用户在执行Match语句之前需要通过Use Graph语句指定当前查询的图。
### Syntax

```sql
USE GRAPH Identifier
```
### Example

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
## Use Instance
Instance类似于Hive/Mysql中的Database的概念。我们可以通过**Use Instance**命令指定当前语句的实例。
### Syntax

```sql
USE INSTANCE Identifier
```
### Example

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