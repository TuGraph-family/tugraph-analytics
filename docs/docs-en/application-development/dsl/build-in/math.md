Geaflow supports the following mathematical operations.

| Operation | Description |
| :--- | :--- |
| + numeric | Return the positive value of numeric |
| - numeric | Return the negative value of numeric |
| numeric1 + numeric2 | Return the result of numeric1 plus numeric2 |
| numeric1 - numeric2 | Return the result of numeric1 minus numeric2 |
| numeric1 * numeric2 | Return the result of numeric1 multiply numeric2 |
| numeric1 / numeric2 | Return the result of numeric1 divid numeric2. If numeric1 and numeric2 are integers, they are evenly divided. e.g. 3/2 = 1, 3/2.0 = 1.5 |
| POWER(numeric1, numeric2) | Return the result of numeric1 raised to the numeric2 |
| ABS(numeric) | Return the absolute value of numeric |
| MOD(numeric1, numeric2) | Return the remainder of numeric1/numeric2 |
| SQRT(numeric) | Return the square root of numeric |
| LN(numeric) | Return the natural logarithm of numeric to base `e` |
| LOG10(numeric) | Return the natural logarithm of numeric to base 10 |
| EXP(numeric) | Return the result of `e` raised to the numeric |
| CEIL(numeric) | Return the smallest integer value greater than or equal to numeric |
| FLOOR(numeric) | Return the biggest integer value less than or equal to numeric |
| SIN(numeric) | Return the sine of numeric |
| COS(numeric) | Return the cosine of numeric |
| TAN(numeric) | Return the tangent of numeric |
| COT(numeric) | Return the cotangent of numeric |
| ASIN(numeric) | Return the arc sine of numeric |
| ACOS(numeric) | Return the arc cosine of numeric |
| ATAN(numeric) | Return the arc tangent of numeric |
| DEGREES(numeric) | Returns the degree of numeric, converted from radians to degrees |
| RADIANS(numeric) | Returns the radian of numeric, converted from degrees to radians |
| SIGN(numeric) | Return the sign of numeric, 1 represents a positive number, -1 represents a negative number, and 0 represents 0 |
| PI | Return the constant value of `PI` |
| E() | Return the constant value of `e` |
| RAND() | Return a random double number between 0-1 |
| RAND(seed s) | Use the initial seed to return pseudo random double values between 0.0 (inclusive) and 1.0 (exclusive). If two RAND functions have the same initial seed, both RAND functions will return the same sequence of numbers |
| RAND_INTEGER(bound numeric) | Return a random integer number between `0-numeric` |
| RAND_INTEGER(seed s, bound numeric) | Use the initial seed to return pseudo random integer values between 0 (inclusive) and `numeric` (exclusive). If two RAND_INTEGER functions have the same initial seed, both RAND_INTEGER functions will return the same sequence of numbers |
|ROUND(numeric1, numeric2)| Return the result of rounding the argument numeric1 to numeric2 decimal places|
| log2(numeric) | Return the natural logarithm of numeric to base 2 |