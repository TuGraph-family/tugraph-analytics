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
	user_name VARCHAR,
	count_id int,
	sum_id int,
	max_id int,
	min_id int,
	avg_id DOUBLE,
	distinct_id int
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO tbl_result
SELECT
  o.user_name,
  count(1) as count_id,
  1 as sum_id,
  1 as min_id,
  1 as max_id,
  1.0 as avg_id,
  1 as distinct_id
FROM orders o
GROUP BY o.user_name;




