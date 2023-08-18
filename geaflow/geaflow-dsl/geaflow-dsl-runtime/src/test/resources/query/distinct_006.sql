CREATE TABLE req (
	id int,
	s varchar
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/request_with_repeat.txt'
);

CREATE TABLE tbl_result (
	id int,
	s varchar
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO tbl_result
SELECT DISTINCT *
FROM req r
ORDER BY r.id
;