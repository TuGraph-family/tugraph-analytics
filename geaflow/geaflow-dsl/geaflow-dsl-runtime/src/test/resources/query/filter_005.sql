CREATE TABLE orders (
	createTime bigint,
	productId bigint,
	orderId bigint,
	units bigint,
	user_name VARCHAR
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/orders.txt'
);

CREATE TABLE output_console(
	f0 VARCHAR,
	f1 bigint,
	f2 bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO output_console
SELECT *
FROM (
 SELECT
    user_name,
    units,
    count(1) as cnt
 FROM orders o
 GROUP BY user_name,units
 HAVING user_name is not null
) a
WHERE a.units % 2 = 0 and a.user_name != '\0\'\\\%\_\"\b\r\t\u001A';;

