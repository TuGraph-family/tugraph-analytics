CREATE TABLE users (
	id long,
	name string,
	age long
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/users_duplication.txt'
);

CREATE TABLE tbl_result (
  f0 varchar,
  f1 bigint,
  f2 bigint,
  f3 bigint,
  f4 bigint,
  f5 double
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO tbl_result
SELECT
    name,
    count(distinct id),
    count(distinct id),
    count(DISTINCT case when id%2 =0 then id else null end),
    count(DISTINCT case when id%2 =1 then id else null end),
    sum(distinct age)
from users
group by name
;
