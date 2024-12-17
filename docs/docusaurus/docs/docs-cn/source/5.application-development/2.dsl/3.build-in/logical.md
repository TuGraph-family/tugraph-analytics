# Logical

Geaflow支持以下逻辑运算：

操作|描述
----------------------|------
boolean1 OR boolean2 | 如果boolean1为true或boolean2为true，则返回true。
boolean1 AND boolean2 | 仅在boolean1为true和boolean2为true时才返回true。
NOT boolean | 返回给定布尔变量的NOT操作的结果。
boolean IS FALSE | 如果布尔变量为false，则返回true。如果布尔变量是UNKNOWN，则返回false。
boolean IS NOT FALSE | 如果布尔变量为true，则返回true。如果布尔变量是UNKNOWN，则返回true。
boolean IS TRUE | 如果布尔变量为true，则返回true。如果布尔变量是UNKNOWN，则返回false。
boolean IS NOT TRUE | 如果布尔变量为false，则返回true。如果布尔变量是UNKNOWN，则返回true。
value1 = value2 | 如果value1等于value2，则返回true。
value1 <> value2 | 如果value1不等于value2，则返回true。
value1 > value2 | 如果value1大于value2，则返回true。
value1 >= value2 | 如果value1大于或等于value2，则返回true。
value1 < value2 | 如果value1小于value2，则返回true。
value1 <= value2 | 如果value1小于或等于value2，则返回true。
value IS NULL | 如果value为null，则返回true。
value IS NOT NULL | 如果value不为null，则返回true。
value1 IS DISTINCT FROM value2 | 如果value1与value2不同，则返回true。如果value1和value2都为null，则它们被视为相等。
value1 IS NOT DISTINCT FROM value2 | 如果value1等于value2，则返回true。如果value1和value2都为null，则它们被视为相等。
value1 BETWEEN value2 AND value3 | 如果value1大于或等于value2且小于value3，则返回true。
value1 NOT BETWEEN value2 AND value3 | 如果value1小于value2或大于或等于value3，则返回true。
string1 LIKE string2 [ ESCAPE string3 ] | 对字符串string1进行模糊匹配，如果匹配到模式string2则返回true，如果不匹配则返回false。
string1 NOT LIKE string2 [ ESCAPE string3 ] | 对字符串string1进行模糊匹配，如果匹配到模式string2则返回false，如果不匹配则返回true。
value IN (value [, value]* ) | 如果value等于列表中的任何一个值，则返回true。
value NOT IN (value [, value]* ) | 如果value不等于列表中的任何一个值，则返回true。
