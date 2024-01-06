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

CREATE TABLE tbl_result (
	createTime bigint,
	productId bigint,
	orderId bigint,
	units bigint,
	user_name VARCHAR
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO tbl_result
SELECT DISTINCT
  o.createTime,
  o.productId,
  o.orderId,
  o.units,
  o.user_name
FROM orders o;




