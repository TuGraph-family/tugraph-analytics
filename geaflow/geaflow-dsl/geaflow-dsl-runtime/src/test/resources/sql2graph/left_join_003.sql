CREATE TABLE IF NOT EXISTS left_join_003_result (
	f0 bigint,
	f1 bigint,
  f2 bigint,
  f3 bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH g_student;

INSERT INTO left_join_003_result
SELECT table_left.sid, table_right.sid, courseCount, cid from
(
SELECT student.id as sid, count(distinct selectCourse.targetId) as courseCount from
student LEFT JOIN selectCourse
on student.id = selectCourse.srcId
group by student.id
) table_left
LEFT JOIN
(
SELECT student.id as sid, selectCourse.targetId as cid from
student, selectCourse
where student.id = selectCourse.srcId
) table_right
on table_left.sid = table_right.sid
;