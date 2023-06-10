The table function returns a list of rows for each input.
**Table Function Syntax**

```
SELECT expr (, expr)*
FROM (Table | SubQuery),
LATERAL TABLE '('TableFunctionRef')' AS Identifier '(' Identifier (,Identifier)* ')'
```

# split
**Syntax**

```sql
	split(string text)
	split(string text, string separator)
```
**Description**
Split the text to a list of single string by the separator. The default separator is: ','.

**Example**

```sql
SELECT t.id, u.name FROM users u, LATERAL table(split(u.ids)) as t(id);
SELECT t.id, u.name FROM users u, LATERAL table(split(u.ids, ',')) as t(id);
```