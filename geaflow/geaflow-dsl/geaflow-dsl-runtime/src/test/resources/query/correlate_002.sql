CREATE function UDTF_SPLIT as 'com.antgroup.geaflow.dsl.runtime.query.udtf.SplitMap';

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

CREATE TABLE console (
	f1 bigint,
	f2 bigint,
	f3 bigint,
	f4 bigint,
	name varchar
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO console
SELECT
	createTime,
	productId,
	orderId,
	units,
	t.name
FROM users, LATERAL table(UDTF_SPLIT(user_name, '|')) as t(name);
