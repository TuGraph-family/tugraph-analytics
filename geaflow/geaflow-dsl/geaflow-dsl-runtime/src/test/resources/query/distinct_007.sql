CREATE TABLE req (
	id int,
	s varchar
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/request_with_repeat.txt'
);

CREATE TABLE tbl_result (
	id int
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO tbl_result
SELECT id
FROM req r
GROUP BY id
ORDER BY r.id
;