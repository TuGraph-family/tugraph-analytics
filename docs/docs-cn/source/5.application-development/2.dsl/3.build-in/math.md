# Math

Geaflow 支持以下数学运算。

| 操作 | 说明 |
| :--- | :--- |
| + numeric | 返回 numeric 的正值 |
| - numeric | 返回 numeric 的负值 |
| numeric1 + numeric2 | 返回 numeric1 加上 numeric2 的结果 |
| numeric1 - numeric2 | 返回 numeric1 减去 numeric2 的结果 |
| numeric1 * numeric2 | 返回 numeric1 乘以 numeric2 的结果 |
| numeric1 / numeric2 | 将numeric1除以numeric2并返回结果。如果numeric1和numeric2都是整数，则它们会被平均分配。例如，3/2 = 1，3/2.0 = 1.5 |
| POWER(numeric1, numeric2) | 返回numeric1的numeric2次方的结果 |
| ABS(numeric) | 返回numeric的绝对值 |
| MOD(numeric1, numeric2) | 返回numeric1除以numeric2的余数 |
| SQRT(numeric) | 返回numeric的平方根 |
| LN(numeric) | 返回numeric以自然对数e为底的对数 |
| LOG10(numeric) | 返回numeric以10为底的对数 |
| EXP(numeric) | 返回e的numeric次幂的结果 |
| CEIL(numeric) | 返回大于或等于numeric的最小整数值 |
| FLOOR(numeric) | 返回小于或等于numeric的最大整数值 |
| SIN(numeric) | 返回numeric的正弦值 |
| COS(numeric) | 返回numeric的余弦值 |
| TAN(numeric) | 返回numeric的正切值 |
| COT(numeric) | 返回numeric的余切值 |
| ASIN(numeric) | 返回numeric的反正弦值 |
| ACOS(numeric) | 返回numeric的反余弦值 |
| ATAN(numeric) | 返回numeric的反正切值 |
| DEGREES(numeric) | 将弧度转换为度数，返回numeric的度数值 |
| RADIANS(numeric) | 将度数转换为弧度，返回numeric的弧度值 |
| SIGN(numeric) | 返回numeric的符号，1表示正数，-1表示负数，0表示0 |
| PI | 返回常量值 `PI` |
| E() | 返回常量值 `e` |
| RAND() | 返回0-1之间的随机双精度浮点数 |
| RAND(seed s) | 使用初始种子返回伪随机double值，介于0.0（含）和1.0（不含）之间。如果两个RAND函数具有相同的初始种子，则两个RAND函数将返回相同的数字序列 |
| RAND_INTEGER(bound numeric) | 返回介于0-numeric之间的随机整数 |
| RAND_INTEGER(seed s, bound numeric) | 使用初始种子返回介于0（含）和numeric（不含）之间的伪随机整数值。如果两个RAND_INTEGER函数具有相同的初始种子，则两个RAND_INTEGER函数将返回相同的数字序列 |
| ROUND(numeric1, numeric2) | 将numeric1四舍五入为numeric2位小数的结果 |
| log2(numeric) | 返回numeric以2为底的对数 |