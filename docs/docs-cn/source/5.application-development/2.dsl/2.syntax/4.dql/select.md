# Select 

## Syntax

```sql
SELECT [ DISTINCT ]
{ * | expr (, expr )* }
FROM { Table | SubQuery | Match }
[ WHERE boolExpr ]
[ Group By expr (',' expr)* ]
[ HAVING boolExpr ]
[ ORDER BY (expr [ASC|DESC]) (',' expr [ASC|DESC])* ]
[ LIMIT number ]
```

## Example
### Select
```sql
SELECT id, name, age FROM user;

SELECT DISTINCT id, name, age FROM user;

SELECT price * 10 FROM trade;
```

### From
#### From Table
```sql
SELECT id, name, age FROM user where id > 10
```
#### From SubQuery
```sql
SELECT id, name, age 
FROM (
	SELECT * FROM user where id > 10
)
```
#### From Match
```sql
SELECT
	a_id,
	weight,
	b_id
FROM (
  MATCH (a) -[e:knows]->(b:person where b.id != 1)
  RETURN a.id as a_id, e.weight as weight, b.id as b_id
)
```
More information about match, please see the Match Syntax.

### Where
```sql
SELECT id, name, age FROM user where id > 10;

SELECT DISTINCT id, name, age FROM user where id > 10;

SELECT price * 10 FROM trade where price > 20;
```
### Group By
```sql
SELECT age, count(id) as cnt FROM user GROUP BY age;

SELECT type, max(age), min(age), avg(age) FROM user GROUP BY type;
```
### Having
```sql
SELECT age, count(id) as cnt FROM user GROUP BY age Having count(id) > 10;
```
### Order By
```sql
SELECT * from user order by age;

SELECT age, count(id) as cnt FROM user GROUP BY age Having count(id) > 10 Order by cnt;
```
### Limit
```sql
SELECT * from user order by age limit 10;

SELECT * from user limit 10;
```