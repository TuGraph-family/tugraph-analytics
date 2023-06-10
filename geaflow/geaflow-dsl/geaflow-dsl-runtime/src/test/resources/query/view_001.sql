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
	f0 bigint,
	f1 bigint,
	f2 bigint,
	f3 bigint,
	f4 bigint,
	f5 varchar
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

CREATE VIEW IF NOT EXISTS console (a, b, c, d, e, f) AS
SELECT
  COUNT(o.productId) AS count_id,
  SUM(o.productId) AS sum_id,
  MAX(o.productId) AS max_id,
  MIN(o.productId) AS min_id,
  COUNT(DISTINCT o.productId) AS distinct_id,
  o.user_name
FROM orders o
WHERE o.units > 10
GROUP BY o.user_name;

INSERT INTO output_console
SELECT * FROM console
where f is not null and f <> 'test' ;

