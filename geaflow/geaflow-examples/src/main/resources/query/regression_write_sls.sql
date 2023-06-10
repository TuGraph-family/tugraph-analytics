CREATE TABLE tables (
  f1 bigint,
	f2 bigint
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/input/email_edge'
);

CREATE TABLE tbl_result (
  f1 bigint,
	f2 bigint
) WITH (
	type = 'sls'
);

INSERT INTO tbl_result
select f1, f2 from tables
;