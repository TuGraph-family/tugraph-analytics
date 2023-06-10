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

CREATE OR REPLACE VIEW console (r, t, c) AS
SELECT
  units,
  orderId,
  productId
FROM orders;

CREATE OR REPLACE VIEW console_1 (a, b, c) AS
SELECT
  r * 2,
  t * 3,
  c + 4
FROM console;

CREATE TABLE output_console (
	console_f1 bigint,
	console_f2 bigint,
	console_f3 bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

CREATE TABLE output_console_1 (
	console_f1 bigint,
	console_f2 bigint,
	console_f3 bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

--INSERT INTO output_console
--SELECT
--	r * 2,
--	t,
--	c
--FROM console;

INSERT INTO output_console_1
SELECT
	a,
	b,
	c
FROM console_1;