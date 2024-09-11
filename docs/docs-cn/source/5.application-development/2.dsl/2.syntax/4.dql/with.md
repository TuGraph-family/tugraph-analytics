# With 
## Syntax
With语句用于指定图计算的起点和相关参数集合，一般和Match语句配合使用，指定Match语句的起始点。
```sql
WITH Identifier AS '(' SubQuery ')'
```

## Example

```sql
SELECT
	a_id,
	b_id,
	weight
FROM (
  WITH p AS (
    SELECT * FROM (VALUES(1, 0.4), (4, 0.5)) AS t(id, weight)
  )
  MATCH (a:person where a.id = p.id) -[e where weight > p.weight + 0.1]->(b)
  RETURN a.id as a_id, e.weight as weight, b.id as b_id
);


```