CREATE TABLE IF NOT EXISTS left_join_005_result (
	f0 bigint,
	f1 bigint,
	f2 bigint,
  f3 bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH g_student;

INSERT INTO left_join_005_result
SELECT sid, fid, table_right.sid2, fid2 from
(
SELECT student.id as sid, knows.targetId as fid from
student, knows
where student.id = knows.srcId
) table_left
LEFT JOIN
(
SELECT student.id as sid2, knows.targetId as fid2 from
student, knows
where student.id = knows.srcId
) table_right
on table_left.fid = table_right.sid2
;