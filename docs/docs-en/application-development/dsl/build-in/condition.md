GeaFlow support both **Case** And **If** condition functions.
* [Case](#Case)
* [If](#If)

# Case
**Syntax**

```sql
CASE expression
    WHEN condition1 THEN result1
    [WHEN condition2 THEN result2]
    ...
    [WHEN conditionN THEN resultN]
    [ELSE result]
END
```
OR

```
CASE WHEN condition1 THEN result1
    [WHEN condition2 THEN result2]
    ...
    [WHEN conditionN THEN resultN]
    [ELSE result]
END
```
**Description**
Returns the expression result when the case-when branch match the condtion.

**Example**

```sql
CASE a
	WHEN 1 THEN '1'
	WHEN 2 THEN '2'
	ELSE '3'
END

CASE WHEN a = 1 THEN '1'
     WHEN a = 2 THEN '2'
	 ELSE '3'
END
```

# If
**Syntax**

```sql
IF (expression, trueValue, falseValue)
```
**Description**
Returns trueValue when the expression is true, otherwise return falseValue.

**Example**

```sql
if(a = 1, -1, a)
```