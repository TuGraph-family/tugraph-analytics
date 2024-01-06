CREATE TABLE tbl_result (
	user_name varchar,
	user_count bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO tbl_result VALUES ('json', 111)
;
