# WITH

The with clause is used to specified the request vertex id and paramters for the graph traversal.

```sql
WITH Identifier AS '(' SubQuery ')'
```

# Example

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
The Match statement will be request by each rows in the with subquery. The result is equivalent to the result of each record firing the Match statement separately.