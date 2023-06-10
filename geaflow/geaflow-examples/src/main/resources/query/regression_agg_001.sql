CREATE TABLE IF NOT EXISTS tables (
	f1 bigint,
	f2 bigint
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/input/email_edge'
);

CREATE TABLE IF NOT EXISTS tbl_result (
  f1 bigint,
	f2 bigint,
	f3 bigint
) WITH (
	type ='file'
);

INSERT INTO tbl_result
select
  f1, sum(f2) as f2_sum, sum(cnt) as cnt_sum
FROM (
  select f1, f2, count(1) as cnt from tables u  group by f1, f2
) a
GROUP BY f1
ORDER BY f1, f2_sum, cnt_sum
;