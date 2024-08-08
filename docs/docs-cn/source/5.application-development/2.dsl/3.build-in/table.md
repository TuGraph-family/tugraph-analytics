# Table

TABLE 函数对每个输入返回若干行数据。

**Table Function Syntax**

```
SELECT expr (, expr)*
FROM (Table | SubQuery),
LATERAL TABLE '('TableFunctionRef')' AS Identifier '(' Identifier (,Identifier)* ')'
```

## split
**Syntax**

```sql
	split(string text)
	split(string text, string separator)
```
**Description**
将文本按分隔符拆分为字符串列表。默认分隔符为英文逗号','。

**Example**

```sql
SELECT t.id, u.name FROM users u, LATERAL table(split(u.ids)) as t(id);
SELECT t.id, u.name FROM users u, LATERAL table(split(u.ids, ',')) as t(id);
```