CREATE TABLE IF NOT EXISTS left_join_001_result (
	f0 bigint,
	f1 bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH g_student;

INSERT INTO left_join_001_result
SELECT student.id as sid, count(distinct selectCourse.targetId) as courseCount from
student LEFT JOIN selectCourse
on student.id = selectCourse.srcId
group by student.id
order by courseCount desc, sid
;