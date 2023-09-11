CREATE function UDTF_SPLIT as 'com.antgroup.geaflow.dsl.runtime.query.udtf.Split2';

CREATE TABLE users (
	createTime bigint,
	productId bigint,
	orderId bigint,
	units bigint,
	user_name VARCHAR
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/users_correlate2.txt'
);

CREATE TABLE tbl_result (
	f1 bigint,
	f2 bigint,
	f3 bigint,
	f4 bigint,
	name varchar,
	alias varchar
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
	t1.name,
	t2.alias
FROM users, LATERAL table(UDTF_SPLIT(user_name, "|", ":")) as t1(name, alias)
, LATERAL table(UDTF_SPLIT(user_name, "|", ":")) as t2(name, alias);
