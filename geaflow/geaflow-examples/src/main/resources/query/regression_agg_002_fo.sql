CREATE TABLE IF NOT EXISTS tables (
	f1 bigint,
	f2 bigint
) WITH (
	type = 'sls'
);

CREATE TABLE IF NOT EXISTS tbl_result (
  groupKey bigint,
	cnt DOUBLE
) WITH (
	type ='file'
);

INSERT INTO tbl_result
select f1 as groupKey, MAX(f2) as cnt from tables u group by groupKey
;