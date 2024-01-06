CREATE function UDTF_SPLIT as 'com.antgroup.geaflow.dsl.udf.table.udtf.Split';

CREATE TABLE users (
	createTime bigint,
	productId bigint,
	orderId bigint,
	units bigint,
	user_name VARCHAR
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/users_correlate.txt'
);

CREATE OR REPLACE VIEW console_view (f1, f2, f3, f4, name) AS
SELECT
	createTime,
	productId,
	orderId,
	units,
	t.name
FROM users, LATERAL table(UDTF_SPLIT(user_name, '|')) as t(name)
;

CREATE TABLE tbl_result (
	f1 bigint,
	f2 bigint,
	f3 bigint,
	f4 bigint,
	name varchar
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO tbl_result
SELECT f1, f2, f3, f4, name FROM console_view
where f2 = 1 OR name = '中国';
