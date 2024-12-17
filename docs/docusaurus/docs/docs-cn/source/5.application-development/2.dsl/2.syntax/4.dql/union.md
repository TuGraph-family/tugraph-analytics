# Union 

## Syntax

```sql
select_statement
UNION [ ALL ]
select_statement

```
## Example

```sql
SELECT * FROM (
	SELECT * FROM user WHERE id < 10
	UNION ALL
	SELECT * FROM user WHERE id > 15
);

SELECT * FROM (
	SELECT * FROM user WHERE id < 10
	UNION
	SELECT * FROM user WHERE id > 15
);

SELECT * FROM (
	SELECT * FROM user WHERE id % 3 = 0
	UNION ALL
	SELECT * FROM user WHERE id % 3 = 1
	UNION ALL
	SELECT * FROM user WHERE id % 3 = 2
);

```