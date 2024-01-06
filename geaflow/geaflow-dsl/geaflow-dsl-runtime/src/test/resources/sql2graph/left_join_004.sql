CREATE TABLE IF NOT EXISTS left_join_004_result (
	f0 bigint,
	f1 bigint,
  f2 bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH g_student;

INSERT INTO left_join_004_result
SELECT sid, table_left.targetId, table_right.id as cid from
(
SELECT student.id as sid, inClass.targetId from
student, inClass
where student.id = inClass.srcId
) table_left
LEFT JOIN gradeClass table_right
on table_left.targetId = table_right.id
;