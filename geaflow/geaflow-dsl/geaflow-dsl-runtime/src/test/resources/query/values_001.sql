CREATE TABLE console (
	user_name varchar,
	user_count bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO console SELECT
concat('j', 's'||'o'||'n'),
case when 1 >= 0 is true then
CAST(CEIL(FLOOR(PI + CHARACTER_LENGTH(LOWER(UPPER("xxx")))
+ ABS(-2) + LN(2) + LOG10(2) + EXP(1)
+ SIN(1) + COS(1) + TAN(1) + COT(1)
+ ASIN(1) + ACOS(1) + ATAN(1) + SIGN(1)
+ DEGREES(1) + RADIANS(1)
+ RAND(1) + RAND_INTEGER(1)))  * 0 / 2 + POWER(10,2) + 11 as bigint)
when 1 <= 0 is not false then 0
else 2 end
;
