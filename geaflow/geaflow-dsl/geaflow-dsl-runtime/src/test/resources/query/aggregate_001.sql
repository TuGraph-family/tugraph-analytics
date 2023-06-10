Create Function rename_count AS 'com.antgroup.geaflow.dsl.udf.table.agg.Count';

CREATE TABLE users (
	id bigint,
	name varchar,
	age int
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/users.txt'
);

CREATE TABLE console (
  groupId int,
	cnt bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO console
SELECT age % 2 as groupId, rename_count(id) as cnt FROM users GROUP BY groupId order by cnt
;