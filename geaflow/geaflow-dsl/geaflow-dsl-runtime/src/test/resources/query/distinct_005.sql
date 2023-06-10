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

CREATE TABLE console (
	min_orderId bigint,
	user_name VARCHAR
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO console
SELECT MIN(orderId), user_name
FROM orders o
GROUP BY user_name;




