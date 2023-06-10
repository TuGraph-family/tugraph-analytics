CREATE TABLE users (
	rt bigint,
	f1 bigint,
	f2 double,
	f3 varchar
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/users2.txt'
);

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
	f1 bigint,
	f2 bigint,
	f3 bigint,
	f4 bigint,
	f5 bigint,
	f6 bigint,
	user_name varchar
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

CREATE VIEW console (count_id, sum_id, max_id, min_id, avg_id, distinct_id, user_name) AS
SELECT
  o.productId AS count_id,
  o.productId AS sum_id,
  o.productId AS max_id,
  o.productId AS min_id,
  o.productId AS avg_id,
  o.productId AS distinct_id,
o.user_name
FROM orders o
WHERE o.units > 10
GROUP BY o.productId, o.user_name;

INSERT INTO output_console
SELECT count_id, sum_id, max_id, min_id, avg_id, distinct_id, user_name FROM console;

