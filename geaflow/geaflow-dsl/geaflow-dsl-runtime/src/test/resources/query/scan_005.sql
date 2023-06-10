CREATE TABLE orders (
	createTime BIGINT,
  productId BIGINT,
  orderId BIGINT,
  units BIGINT,
  user_name VARCHAR
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/orders.txt'
);

CREATE TABLE output_console(
	  createTime BIGINT,
    productId BIGINT,
    orderId BIGINT,
    units BIGINT,
    user_name VARCHAR
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO output_console
SELECT * FROM orders;