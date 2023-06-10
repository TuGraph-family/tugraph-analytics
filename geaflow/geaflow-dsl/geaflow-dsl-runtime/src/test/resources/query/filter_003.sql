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
	createTime bigint,
	productId bigint,
	orderId bigint,
	units bigint,
	user_name VARCHAR
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO output_console
SELECT
	createTime,
	productId,
	orderId,
	units,
	user_name
FROM orders
WHERE (productId=10 AND orderId>3) OR (productId<8)
AND user_name LIKE 'hel' OR user_name in ('hello', 'mills', 'c', 'd', 'e', 'f',
'g', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'a', 'b', 'c', 'd', 'e', 'f', 'g')
OR user_name LIKE '%a%b%c%' OR user_name LIKE 'abc\\%de%';