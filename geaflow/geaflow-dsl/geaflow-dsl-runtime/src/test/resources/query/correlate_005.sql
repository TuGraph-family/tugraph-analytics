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
SELECT
	createTime,
	productId,
	orderId,
	units,
	t.name
FROM users, LATERAL table(UDTF_SPLIT(user_name, '|')) as t(name)
where name = '中国';
