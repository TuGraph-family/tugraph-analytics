CREATE TABLE orders(
	createTime bigint,
	productId bigint,
	orderId bigint,
	units bigint,
	user_name VARCHAR
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/orders.txt'
);

CREATE TABLE tbl_result (
	user_name varchar,
	user_count bigint,
	user_units bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO tbl_result
SELECT
    user_name,
    orderId,
    units
FROM orders
ORDER BY orderId - units desc
limit 10;




