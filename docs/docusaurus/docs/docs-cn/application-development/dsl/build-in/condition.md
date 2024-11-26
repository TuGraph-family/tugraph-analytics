GeaFlow支持**Case**和**If**条件函数。
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
当Case函数中的某个分支与条件匹配时，返回表达式的结果。

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
当表达式为真时，返回trueValue的值，否则返回falseValue的值。

**Example**

```sql
if(a = 1, -1, a)
```
